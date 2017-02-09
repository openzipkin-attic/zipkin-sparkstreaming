/**
 * Copyright 2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.sparkstreaming;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.Util;
import zipkin.storage.StorageComponent;

@AutoValue
public abstract class SparkStreamingJob implements Closeable {

  public static Builder newBuilder() {
    Map<String, String> sparkProperties = new LinkedHashMap<>();
    sparkProperties.put("spark.ui.enabled", "false");
    // avoids strange class not found bug on Logger.setLevel
    sparkProperties.put("spark.akka.logLifecycleEvents", "true");
    return new AutoValue_SparkStreamingJob.Builder()
        .sparkMaster("local[*]")
        .sparkJars(Collections.emptyList())
        .sparkProperties(sparkProperties)
        .batchDuration(10_000);
  }

  @AutoValue.Builder
  public interface Builder {
    /**
     * The spark master used for this job. Defaults to "local[*]"
     *
     * <p>local[*] master lets us run & test the job locally without setting a Spark cluster
     */
    Builder sparkMaster(String sparkMaster);

    /** When set, this indicates which sparkJars to distribute to the cluster. */
    Builder sparkJars(List<String> sparkJars);

    /** Overrides the properties used to create a {@link SparkConf}. */
    Builder sparkProperties(Map<String, String> sparkProperties);

    /** The time interval at which streaming data will be divided into batches. Defaults to 10s. */
    Builder batchDuration(long batchDurationMillis);

    /** Produces a stream of serialized span messages (thrift or json lists) */
    Builder messageStreamFactory(MessageStreamFactory messageStreamFactory);

    /** Accepts spans grouped by trace ID. For example, writing to a {@link StorageComponent} */
    Builder traceConsumer(TraceConsumer traceConsumer);

    SparkStreamingJob build();
  }

  abstract String sparkMaster();

  abstract List<String> sparkJars();

  abstract Map<String, String> sparkProperties();

  abstract long batchDuration();

  abstract MessageStreamFactory messageStreamFactory();

  abstract TraceConsumer traceConsumer();

  final AtomicBoolean started = new AtomicBoolean(false);

  @Memoized
  JavaStreamingContext jsc() {
    SparkConf conf = new SparkConf(true)
        .setMaster(sparkMaster())
        .setAppName(getClass().getName());
    if (!sparkJars().isEmpty()) conf.setJars(sparkJars().toArray(new String[0]));
    for (Map.Entry<String, String> entry : sparkProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return new JavaStreamingContext(conf, new Duration(batchDuration()));
  }

  /** Starts the streaming job. Use {@link #close()} to stop it */
  public SparkStreamingJob start() {
    if (!started.compareAndSet(false, true)) return this;

    streamSpansToStorage(
        messageStreamFactory().create(jsc()),
        traceConsumer()
    );

    jsc().start();
    return this;
  }

  /** Use this to block on {@link #close()} */
  public void awaitTermination() {
    if (started.get()) jsc().awaitTermination();
  }

  // NOTE: this is intentionally static to remind us that all state passed in must be serializable
  // Otherwise, tasks cannot be distributed across the cluster.
  static void streamSpansToStorage(
      JavaDStream<byte[]> messageStream,
      TraceConsumer traceConsumer
  ) {
    JavaDStream<Span> spans = messageStream
        .filter(bytes -> bytes.length > 0)
        .flatMap(bytes -> {
          try {
            return readSpans(bytes);
          } catch (RuntimeException e) {
            return Collections.<Span>emptyList();
          }
        });

    // TODO: plug in some filter to drop spans regardless of trace ID
    // spans = spans.filter(spanFilter);

    JavaPairDStream<String, Iterable<Span>> tracesById = spans
        .mapToPair(s -> new Tuple2<>(Util.toLowerHex(s.traceIdHigh, s.traceId), s))
        .groupByKey();

    tracesById.foreachRDD(rdd -> {
      rdd.values().foreachPartition(p -> {
        while (p.hasNext()) {
          Iterable<Span> trace = p.next();
          traceConsumer.accept(p.next());
        }
      });
    });
  }

  // In TBinaryProtocol encoding, the first byte is the TType, in a range 0-16
  // .. If the first byte isn't in that range, it isn't a thrift.
  //
  // When byte(0) == '[' (91), assume it is a list of json-encoded spans
  //
  // When byte(0) <= 16, assume it is a TBinaryProtocol-encoded thrift
  // .. When serializing a Span (Struct), the first byte will be the type of a field
  // .. When serializing a List[ThriftSpan], the first byte is the member type, TType.STRUCT(12)
  // .. As ThriftSpan has no STRUCT fields: so, if the first byte is TType.STRUCT(12), it is a list.
  static List<Span> readSpans(byte[] bytes) {
    if (bytes[0] == '[') {
      return Codec.JSON.readSpans(bytes);
    } else {
      if (bytes[0] == 12 /* TType.STRUCT */) {
        return Codec.THRIFT.readSpans(bytes);
      } else { // historical kafka encoding of single thrift span per message
        return Collections.singletonList(Codec.THRIFT.readSpan(bytes));
      }
    }
  }

  @Override public void close() throws IOException {
    jsc().close();
    // not sure how to get spark to close things
    if (traceConsumer() instanceof Closeable) {
      ((Closeable) traceConsumer()).close();
    }
  }

  SparkStreamingJob() {
  }
}

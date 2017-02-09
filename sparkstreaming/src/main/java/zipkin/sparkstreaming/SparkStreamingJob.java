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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

import static zipkin.internal.Util.checkArgument;
import static zipkin.internal.Util.checkNotNull;

public final class SparkStreamingJob implements Closeable {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    MessageStreamFactory messageStreamFactory;
    TraceConsumer traceConsumer;
    String sparkMaster = "local[*]";
    String[] sparkJars;
    Map<String, String> sparkProperties = new LinkedHashMap<>();
    long batchDuration = 10 * 1000;

    Builder() {
      sparkProperties.put("spark.ui.enabled", "false");
      // avoids strange class not found bug on Logger.setLevel
      sparkProperties.put("spark.akka.logLifecycleEvents", "true");
    }

    /** Produces a stream of serialized span messages (thrift or json lists) */
    public Builder spanMessagesFactory(MessageStreamFactory messageStreamFactory) {
      this.messageStreamFactory = checkNotNull(messageStreamFactory, "messageStreamFactory");
      return this;
    }

    /** Accepts spans grouped by trace ID. For example, writing to a {@link StorageComponent} */
    public Builder traceConsumer(TraceConsumer traceConsumer) {
      this.traceConsumer = checkNotNull(traceConsumer, "traceConsumer");
      return this;
    }

    /** The time interval at which streaming data will be divided into batches. Defaults to 10s. */
    public Builder batchDuration(long batchDuration, TimeUnit unit) {
      checkArgument(batchDuration >= 0, "batchDuration < 0: %s", batchDuration);
      this.batchDuration = unit.toMillis(checkNotNull(batchDuration, "batchDuration"));
      return this;
    }

    /**
     * The spark master used for this job. Defaults to "local[*]"
     *
     * <p>local[*] master lets us run & test the job locally without setting a Spark cluster
     */
    public Builder sparkMaster(String sparkMaster) {
      this.sparkMaster = sparkMaster;
      return this;
    }

    /** When set, this indicates which sparkJars to distribute to the cluster. */
    public Builder sparkJars(String... sparkJars) {
      this.sparkJars = sparkJars;
      return this;
    }

    /** Overrides the properties used to create a {@link SparkConf}. */
    public Builder sparkProperties(Map<String, String> sparkProperties) {
      this.sparkProperties = checkNotNull(sparkProperties, "sparkProperties");
      return this;
    }

    public SparkStreamingJob build() {
      return new SparkStreamingJob(this);
    }
  }

  final JavaStreamingContext jsc;
  final TraceConsumer traceConsumer;
  final MessageStreamFactory messageStreamFactory;
  final AtomicBoolean started = new AtomicBoolean(false);

  SparkStreamingJob(Builder builder) {
    messageStreamFactory = checkNotNull(builder.messageStreamFactory, "messageStreamFactory");
    traceConsumer = checkNotNull(builder.traceConsumer, "traceConsumer");
    SparkConf conf = new SparkConf(true)
        .setMaster(builder.sparkMaster)
        .setAppName(getClass().getName());
    if (builder.sparkJars != null) conf.setJars(builder.sparkJars);
    for (Map.Entry<String, String> entry : builder.sparkProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    jsc = new JavaStreamingContext(conf, new Duration(builder.batchDuration));
  }

  public SparkStreamingJob start() {
    if (!started.compareAndSet(false, true)) return this;

    streamSpansToStorage(messageStreamFactory.create(jsc), traceConsumer);

    jsc.start();
    return this;
  }

  // NOTE: this is intentionally static to remind us that all state passed in must be serializable
  // Otherwise, tasks cannot be distributed across the cluster.
  static void streamSpansToStorage(JavaDStream<byte[]> messageStream, TraceConsumer traceConsumer) {
    JavaDStream<Span> decodedSpans = messageStream
        .filter(bytes -> bytes.length > 0)
        .map(bytes -> {
          try {
            return readSpans(bytes);
          } catch (RuntimeException e) {
            return Collections.<Span>emptyList();
          }
        })
        .flatMap(spans -> spans);

    JavaDStream<Span> validSpans = decodedSpans
        .filter(s -> true /** TODO: plug in some filter and metrics.incrementSpansDropped */)
        .map(s -> s /** TODO: plugin some transformer */);

    JavaPairDStream<String, Iterable<Span>> traces = validSpans
        .mapToPair(s -> new Tuple2<>(Util.toLowerHex(s.traceIdHigh, s.traceId), s))
        .groupByKey();

    traces.foreachRDD(rdd -> {
      rdd.foreachPartition(p -> {
        while (p.hasNext()) { // _1 is a trace id and _2 are the spans
          traceConsumer.accept(p.next()._2());
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

  public void awaitTermination() {
    if (started.get()) jsc.awaitTermination();
  }

  @Override public void close() throws IOException {
    jsc.close();
    // not sure how to get spark to close things
    if (traceConsumer instanceof Closeable) {
      ((Closeable) traceConsumer).close();
    }
  }
}

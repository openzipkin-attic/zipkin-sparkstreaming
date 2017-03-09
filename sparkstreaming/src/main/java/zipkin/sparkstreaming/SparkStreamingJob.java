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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
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
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put("spark.ui.enabled", "false");
    return new AutoValue_SparkStreamingJob.Builder()
        .master("local[*]")
        .jars(Collections.emptyList())
        .conf(conf)
        .adjusters(Collections.emptyList())
        .batchDuration(10_000)
        .zipkinLogLevel("INFO");
  }

  @AutoValue.Builder
  public interface Builder {
    /**
     * The spark master used for this job. Defaults to "local[*]"
     *
     * <p>local[*] master lets us run & test the job locally without setting a Spark cluster
     */
    Builder master(String master);

    /** When set, this indicates which jars to distribute to the cluster. */
    Builder jars(List<String> jars);

    /** Overrides the properties used to create a {@link SparkConf}. */
    Builder conf(Map<String, String> conf);

    /** The time interval at which streaming data will be divided into batches. Defaults to 10s. */
    Builder batchDuration(long batchDurationMillis);

    /** Produces a stream of serialized span messages (thrift or json lists) */
    Builder streamFactory(StreamFactory streamFactory);

    /** Conditionally adjusts spans grouped by trace ID. For example, pruning data */
    Builder adjusters(List<Adjuster> adjusters);

    /** Accepts spans grouped by trace ID. For example, writing to a {@link StorageComponent} */
    Builder consumer(Consumer consumer);

    /** Log4J level used for the "zipkin" category. Important when running in a cluster. */
    Builder zipkinLogLevel(String zipkinLogLevel);

    SparkStreamingJob build();
  }

  abstract String master();

  abstract List<String> jars();

  abstract Map<String, String> conf();

  abstract long batchDuration();

  abstract StreamFactory streamFactory();

  abstract List<Adjuster> adjusters();

  abstract Consumer consumer();

  abstract String zipkinLogLevel();

  final AtomicBoolean started = new AtomicBoolean(false);

  @Memoized
  JavaStreamingContext jsc() {
    SparkConf conf = new SparkConf(true)
        .setMaster(master())
        .setAppName(getClass().getName());
    if (!jars().isEmpty()) conf.setJars(jars().toArray(new String[0]));
    for (Map.Entry<String, String> entry : conf().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return new JavaStreamingContext(conf, new Duration(batchDuration()));
  }

  /** Starts the streaming job. Use {@link #close()} to stop it */
  public SparkStreamingJob start() {
    if (!started.compareAndSet(false, true)) return this;

    Runnable logInitializer = LogInitializer.create(zipkinLogLevel());
    logInitializer.run(); // Ensures local log commands emit
    streamSpansToStorage(
        streamFactory().create(jsc()),
        new AutoValue_ReadSpans(logInitializer),
        new AutoValue_AdjustAndConsumeSpansSharingTraceId(logInitializer, adjusters(), consumer())
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
      JavaDStream<byte[]> stream,
      ReadSpans readSpans,
      AdjustAndConsumeSpansSharingTraceId adjustAndConsumeSpansSharingTraceId
  ) {
    JavaDStream<Span> spans = stream.flatMap(readSpans);

    // TODO: plug in some filter to drop spans regardless of trace ID
    // spans = spans.filter(spanFilter);

    JavaPairDStream<String, Iterable<Span>> tracesById = spans
        .mapToPair(s -> new Tuple2<>(Util.toLowerHex(s.traceIdHigh, s.traceId), s))
        .groupByKey();

    tracesById.foreachRDD(rdd -> {
      rdd.values().foreachPartition(adjustAndConsumeSpansSharingTraceId);
    });
  }

  @Override public void close() throws IOException {
    jsc().close();
    // not sure how to get spark to close things
    if (consumer() instanceof Closeable) {
      ((Closeable) consumer()).close();
    }
  }

  SparkStreamingJob() {
  }
}

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
package zipkin.sparkstreaming.stream.kafka;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import kafka.serializer.DefaultDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import zipkin.sparkstreaming.StreamFactory;

import static zipkin.internal.Util.checkNotNull;

/**
 * Ingests Spans form a Kafka topic advertised by Zookeeper
 */
@AutoValue
public abstract class KafkaStreamFactory implements StreamFactory {

  public static Builder newBuilder() {
    return new AutoValue_KafkaStreamFactory.Builder()
        .topic("zipkin")
        .groupId("zipkin");
  }

  @AutoValue.Builder
  public static abstract class Builder {

    /** Kafka topic encoded lists of spans are be consumed from. Defaults to "zipkin" */
    public abstract Builder topic(String topic);

    /** Consumer group this process is consuming on behalf of. Defaults to "zipkin" */
    public abstract Builder groupId(String groupId);

    /**
     * Initial set of kafka servers to connect to; others may be discovered. Values are in host:port
     * syntax. No default.
     *
     * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
     */
    public final Builder bootstrapServers(final List<String> bootstrapServers) {
      checkNotNull(bootstrapServers, "bootstrapServers");
      return bootstrapServers(new BootstrapServers() {
        @Override public List<String> get() {
          return bootstrapServers;
        }

        @Override public String toString() {
          return bootstrapServers.toString();
        }
      });
    }

    /**
     * Like {@link #bootstrapServers(List)}, except the value is deferred.
     *
     * <p>This was added to support dynamic endpoint resolution for Amazon Elasticsearch. This value
     * is only read once.
     */
    public abstract Builder bootstrapServers(BootstrapServers bootstrapServers);

    public abstract KafkaStreamFactory build();

    Builder() {
    }
  }

  abstract String topic();

  abstract String groupId();

  abstract BootstrapServers bootstrapServers();

  @Override public JavaDStream<byte[]> create(JavaStreamingContext jsc) {
    return KafkaUtils.createDirectStream(
        jsc,
        byte[].class,
        byte[].class,
        DefaultDecoder.class,
        DefaultDecoder.class,
        kafkaParams(),
        Collections.singleton(topic()))
        .map(m -> m._2); // get value
  }

  @Memoized
  Map<String, String> kafkaParams() {
    Map<String, String> kafkaParams = new LinkedHashMap<>();
    kafkaParams.put("metadata.broker.list", StringUtils.join(bootstrapServers().get(), ","));
    kafkaParams.put("group.id", groupId());
    return Collections.unmodifiableMap(kafkaParams);
  }

  KafkaStreamFactory() {
  }
}

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
package zipkin.sparkstreaming.job;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Import;
import zipkin.sparkstreaming.SparkStreamingJob;
import zipkin.sparkstreaming.autoconfigure.adjuster.finagle.ZipkinFinagleAdjusterAutoConfiguration;
import zipkin.sparkstreaming.autoconfigure.consumer.storage.ZipkinStorageConsumerAutoConfiguration;
import zipkin.sparkstreaming.autoconfigure.stream.kafka.ZipkinKafkaStreamFactoryAutoConfiguration;

@SpringBootApplication
@Import({
    ZipkinSparkStreamingConfiguration.class,
    // These need to be explicity included as the shade plugin squashes spring.properties
    ZipkinKafkaStreamFactoryAutoConfiguration.class,
    ZipkinFinagleAdjusterAutoConfiguration.class,
    ZipkinStorageConsumerAutoConfiguration.class
})
public class ZipkinSparkStreamingJob {

  public static void main(String[] args) {
    new SpringApplicationBuilder(ZipkinSparkStreamingJob.class)
        .run(args)
        .getBean(SparkStreamingJob.class).awaitTermination();
  }
}

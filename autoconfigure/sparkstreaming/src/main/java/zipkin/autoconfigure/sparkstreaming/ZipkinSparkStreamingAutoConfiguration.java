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
package zipkin.autoconfigure.sparkstreaming;

import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.sparkstreaming.StreamFactory;
import zipkin.sparkstreaming.SparkStreamingJob;
import zipkin.sparkstreaming.Adjuster;
import zipkin.sparkstreaming.Consumer;

@Configuration
@EnableConfigurationProperties(ZipkinSparkStreamingProperties.class)
@ConditionalOnBean({StreamFactory.class, Consumer.class})
public class ZipkinSparkStreamingAutoConfiguration {

  @Autowired(required = false)
  List<Adjuster> adjusters = Collections.emptyList();

  @Bean SparkStreamingJob sparkStreaming(
      ZipkinSparkStreamingProperties sparkStreaming,
      StreamFactory streamFactory,
      Consumer consumer
  ) {
    return sparkStreaming.toBuilder()
        .streamFactory(streamFactory)
        .adjusters(adjusters)
        .consumer(consumer)
        .build()
        .start();
  }
}


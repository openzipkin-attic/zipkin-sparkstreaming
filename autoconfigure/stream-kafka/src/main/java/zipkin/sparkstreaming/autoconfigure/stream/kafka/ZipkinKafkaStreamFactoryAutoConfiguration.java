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
package zipkin.sparkstreaming.autoconfigure.stream.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin.sparkstreaming.StreamFactory;

@Configuration
@EnableConfigurationProperties(ZipkinKafkaStreamFactoryProperties.class)
@Conditional(ZipkinKafkaStreamFactoryAutoConfiguration.KafkaServersSetCondition.class)
public class ZipkinKafkaStreamFactoryAutoConfiguration {

  @Bean StreamFactory kafkaStream(ZipkinKafkaStreamFactoryProperties properties) {
    return properties.toBuilder().build();
  }

  static final class KafkaServersSetCondition extends SpringBootCondition {
    static final String BOOTSTRAP = "zipkin.sparkstreaming.stream.kafka.bootstrap-servers";
    static final String CONNECT = "zipkin.sparkstreaming.stream.kafka.zookeeper.connect";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata a) {
      String bootstrap = context.getEnvironment().getProperty(BOOTSTRAP);
      String connect = context.getEnvironment().getProperty(CONNECT);
      return (bootstrap == null || bootstrap.isEmpty()) && (connect == null || connect.isEmpty()) ?
          ConditionOutcome.noMatch("neither " + BOOTSTRAP + " nor " + CONNECT + " are set") :
          ConditionOutcome.match();
    }
  }
}

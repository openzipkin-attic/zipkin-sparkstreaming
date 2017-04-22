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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin.sparkstreaming.autoconfigure.stream.kafka.ZipkinKafkaStreamFactoryAutoConfiguration;
import zipkin.sparkstreaming.autoconfigure.stream.kafka.ZipkinKafkaStreamFactoryProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinKafkaStreamFactoryAutoConfigurationTest {
  static final String KAFKA_BOOTSTRAP = "127.0.0.1:9092";
  static final String KAFKA_ZOOKEEPER = "127.0.0.1:2181";

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void close() {
    if (context != null) context.close();
  }

  @Test
  public void doesntProvideCollectorComponent_whenKafkaZookeeperUnset() {
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKafkaStreamFactoryAutoConfiguration.class
    );
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(KafkaStreamFactory.class);
  }

  @Test
  public void providesCollectorComponent_whenBootstrapServersSet() {
    addEnvironment(context,
        "zipkin.sparkstreaming.stream.kafka.bootstrap-servers:" + KAFKA_BOOTSTRAP
    );
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKafkaStreamFactoryAutoConfiguration.class
    );
    context.refresh();

    ZipkinKafkaStreamFactoryProperties props =
        context.getBean(ZipkinKafkaStreamFactoryProperties.class);
    assertThat(props.getBootstrapServers())
        .containsExactly(KAFKA_BOOTSTRAP);
  }

  @Test
  public void providesCollectorComponent_whenKafkaZookeeperSet() {
    addEnvironment(context,
        "zipkin.sparkstreaming.stream.kafka.zookeeper.connect:" + KAFKA_ZOOKEEPER
    );
    context.register(
        PropertyPlaceholderAutoConfiguration.class,
        ZipkinKafkaStreamFactoryAutoConfiguration.class
    );
    context.refresh();

    ZipkinKafkaStreamFactoryProperties props =
        context.getBean(ZipkinKafkaStreamFactoryProperties.class);
    assertThat(props.getZookeeper().getConnect())
        .isEqualTo(KAFKA_ZOOKEEPER);
  }
}

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
package zipkin.sparkstreaming.autoconfigure.consumer.storage;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin.sparkstreaming.consumer.storage.StorageConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinStorageConsumerAutoConfigurationTest {

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void close() {
    if (context != null) context.close();
  }

  @Test
  public void providesStorageComponent_whenStorageTypeCassandra() {
    addEnvironment(context,
        "zipkin.sparkstreaming.consumer.storage.fail-fast:false",
        "zipkin.storage.type:cassandra");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinStorageConsumerAutoConfiguration.class);
    context.refresh();

    assertThat(storage()).isInstanceOf(
        ZipkinStorageConsumerAutoConfiguration.CassandraStorageConsumer.class);
  }

  @Test
  public void providesStorageComponent_whenStorageTypeCassandra3() {
    addEnvironment(context,
        "zipkin.sparkstreaming.consumer.storage.fail-fast:false",
        "zipkin.storage.type:cassandra3");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinStorageConsumerAutoConfiguration.class);
    context.refresh();

    assertThat(storage()).isInstanceOf(
        ZipkinStorageConsumerAutoConfiguration.Cassandra3StorageConsumer.class);
  }

  @Test
  public void providesStorageComponent_whenStorageTypeElasticsearch() {
    addEnvironment(context,
        "zipkin.sparkstreaming.consumer.storage.fail-fast:false",
        "zipkin.storage.type:elasticsearch"
    );
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinStorageConsumerAutoConfiguration.class);
    context.refresh();

    assertThat(storage()).isInstanceOf(
        ZipkinStorageConsumerAutoConfiguration.ElasticsearchStorageConsumer.class);
  }

  @Test
  public void providesStorageComponent_whenStorageTypeMysql() {
    addEnvironment(context,
        "zipkin.sparkstreaming.consumer.storage.fail-fast:false",
        "zipkin.storage.type:mysql"
    );
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinStorageConsumerAutoConfiguration.class);
    context.refresh();

    assertThat(storage()).isInstanceOf(
        ZipkinStorageConsumerAutoConfiguration.MySQLStorageConsumer.class);
  }

  /** fail fast is default, which helps discover storage errors before the job runs */
  @Test public void failFast() {
    addEnvironment(context,
        "zipkin.storage.type:elasticsearch",
        "zipkin.storage.elasticsearch.hosts:http://host1:9200"
    );
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinStorageConsumerAutoConfiguration.class);

    thrown.expect(BeanCreationException.class);
    context.refresh();
  }

  StorageConsumer storage() {
    return context.getBean(StorageConsumer.class);
  }
}

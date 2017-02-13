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

import java.io.IOException;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import zipkin.Component;
import zipkin.autoconfigure.storage.cassandra.ZipkinCassandraStorageAutoConfiguration;
import zipkin.autoconfigure.storage.cassandra.ZipkinCassandraStorageProperties;
import zipkin.autoconfigure.storage.cassandra3.ZipkinCassandra3StorageAutoConfiguration;
import zipkin.autoconfigure.storage.cassandra3.ZipkinCassandra3StorageProperties;
import zipkin.autoconfigure.storage.elasticsearch.http.ZipkinElasticsearchHttpStorageAutoConfiguration;
import zipkin.autoconfigure.storage.elasticsearch.http.ZipkinElasticsearchHttpStorageProperties;
import zipkin.autoconfigure.storage.elasticsearch.http.ZipkinElasticsearchOkHttpAutoConfiguration;
import zipkin.autoconfigure.storage.mysql.ZipkinMySQLStorageAutoConfiguration;
import zipkin.autoconfigure.storage.mysql.ZipkinMySQLStorageProperties;
import zipkin.sparkstreaming.consumer.storage.StorageConsumer;
import zipkin.storage.StorageComponent;
import zipkin.storage.cassandra.CassandraStorage;
import zipkin.storage.cassandra3.Cassandra3Storage;
import zipkin.storage.elasticsearch.http.ElasticsearchHttpStorage;
import zipkin.storage.mysql.MySQLStorage;

@Configuration
@ConditionalOnProperty("zipkin.storage.type")
@EnableConfigurationProperties({
    ZipkinCassandraStorageProperties.class,
    ZipkinCassandra3StorageProperties.class,
    ZipkinElasticsearchHttpStorageProperties.class,
    ZipkinMySQLStorageProperties.class
})
@Import({
    ZipkinCassandraStorageAutoConfiguration.class,
    ZipkinCassandra3StorageAutoConfiguration.class,
    ZipkinElasticsearchOkHttpAutoConfiguration.class,
    ZipkinElasticsearchHttpStorageAutoConfiguration.class,
    ZipkinMySQLStorageAutoConfiguration.class
})
public class ZipkinStorageConsumerAutoConfiguration {
  @ConditionalOnBean(StorageComponent.class)
  @Bean StorageConsumer storageConsumer(
      StorageComponent component,
      @Value("${zipkin.sparkstreaming.consumer.storage.fail-fast:true}") boolean failFast,
      BeanFactory bf
  ) throws IOException {
    if (failFast) checkStorageOk(component);

    if (component instanceof ElasticsearchHttpStorage) {
      return new ElasticsearchStorageConsumer(
          bf.getBean(ZipkinElasticsearchHttpStorageProperties.class));
    } else if (component instanceof CassandraStorage) {
      return new CassandraStorageConsumer(
          bf.getBean(ZipkinCassandraStorageProperties.class));
    } else if (component instanceof Cassandra3Storage) {
      return new Cassandra3StorageConsumer(
          bf.getBean(ZipkinCassandra3StorageProperties.class));
    } else if (component instanceof MySQLStorage) {
      return new MySQLStorageConsumer(
          bf.getBean(ZipkinMySQLStorageProperties.class));
    } else {
      throw new UnsupportedOperationException(component + " not yet supported");
    }
  }

  // fail fast because it is easier to detect problems here than after the cluster starts!
  void checkStorageOk(StorageComponent component) throws IOException {
    Component.CheckResult result = component.check();
    if (!result.ok) throw new IllegalStateException("Storage not ok", result.exception);
    component.close(); // we don't use this directly as job instantiates their own
  }

  static final class ElasticsearchStorageConsumer extends StorageConsumer {
    final ZipkinElasticsearchHttpStorageProperties properties;

    ElasticsearchStorageConsumer(ZipkinElasticsearchHttpStorageProperties properties) {
      this.properties = properties;
    }

    @Override protected StorageComponent tryCompute() {
      return properties.toBuilder(new OkHttpClient()).build();
    }
  }

  static final class CassandraStorageConsumer extends StorageConsumer {
    final ZipkinCassandraStorageProperties properties;

    CassandraStorageConsumer(ZipkinCassandraStorageProperties properties) {
      this.properties = properties;
    }

    @Override protected StorageComponent tryCompute() {
      return properties.toBuilder().build();
    }
  }

  static final class Cassandra3StorageConsumer extends StorageConsumer {
    final ZipkinCassandra3StorageProperties properties;

    Cassandra3StorageConsumer(ZipkinCassandra3StorageProperties properties) {
      this.properties = properties;
    }

    @Override protected StorageComponent tryCompute() {
      return properties.toBuilder().build();
    }
  }

  static final class MySQLStorageConsumer extends StorageConsumer {
    final ZipkinMySQLStorageProperties properties;

    MySQLStorageConsumer(ZipkinMySQLStorageProperties properties) {
      this.properties = properties;
    }

    @Override protected StorageComponent tryCompute() {
      return MySQLStorage.builder()
          .executor(Runnable::run) // intentionally blocking
          .datasource(properties.toDataSource()).build();
    }
  }
}


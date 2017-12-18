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
import java.util.Iterator;
import java.util.Properties;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import zipkin.Component;
import zipkin.autoconfigure.storage.cassandra.ZipkinCassandraStorageAutoConfiguration;
import zipkin.autoconfigure.storage.cassandra3.ZipkinCassandra3StorageAutoConfiguration;
import zipkin.autoconfigure.storage.elasticsearch.http.ZipkinElasticsearchHttpStorageAutoConfiguration;
import zipkin.autoconfigure.storage.elasticsearch.http.ZipkinElasticsearchOkHttpAutoConfiguration;
import zipkin.autoconfigure.storage.mysql.ZipkinMySQLStorageAutoConfiguration;
import zipkin.internal.V2StorageComponent;
import zipkin.sparkstreaming.consumer.storage.StorageConsumer;
import zipkin.storage.StorageComponent;
import zipkin.storage.cassandra.CassandraStorage;
import zipkin.storage.elasticsearch.http.ElasticsearchHttpStorage;
import zipkin.storage.mysql.MySQLStorage;

@Configuration
@ConditionalOnProperty("zipkin.storage.type")
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
    Properties properties = extractZipkinProperties(bf.getBean(ConfigurableEnvironment.class));
    if (component instanceof V2StorageComponent) {
      zipkin2.storage.StorageComponent v2Storage = ((V2StorageComponent) component).delegate();
      if (v2Storage instanceof ElasticsearchHttpStorage) {
        return new ElasticsearchStorageConsumer(properties);
      } else if (v2Storage instanceof zipkin2.storage.cassandra.CassandraStorage) {
        return new Cassandra3StorageConsumer(properties);
      } else {
        throw new UnsupportedOperationException(v2Storage + " not yet supported");
      }
    } else if (component instanceof CassandraStorage) {
      return new CassandraStorageConsumer(properties);
    } else if (component instanceof MySQLStorage) {
      return new MySQLStorageConsumer(properties);
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

  static final class ElasticsearchStorageConsumer extends AutoConfigurationStorageConsumer {
    ElasticsearchStorageConsumer(Properties properties) {
      super(properties);
    }

    @Override void registerAutoConfiguration(AnnotationConfigApplicationContext context) {
      context.register(ZipkinElasticsearchOkHttpAutoConfiguration.class);
      context.register(ZipkinElasticsearchHttpStorageAutoConfiguration.class);
    }
  }

  static final class CassandraStorageConsumer extends AutoConfigurationStorageConsumer {
    CassandraStorageConsumer(Properties properties) {
      super(properties);
    }

    @Override void registerAutoConfiguration(AnnotationConfigApplicationContext context) {
      context.register(ZipkinCassandraStorageAutoConfiguration.class);
    }
  }

  static final class Cassandra3StorageConsumer extends AutoConfigurationStorageConsumer {
    Cassandra3StorageConsumer(Properties properties) {
      super(properties);
    }

    @Override void registerAutoConfiguration(AnnotationConfigApplicationContext context) {
      context.register(ZipkinCassandra3StorageAutoConfiguration.class);
    }
  }

  static final class MySQLStorageConsumer extends AutoConfigurationStorageConsumer {
    MySQLStorageConsumer(Properties properties) {
      super(properties);
    }

    @Override void registerAutoConfiguration(AnnotationConfigApplicationContext context) {
      context.register(ZipkinMySQLStorageAutoConfiguration.class);
    }
  }

  static Properties extractZipkinProperties(ConfigurableEnvironment env) {
    Properties properties = new Properties();
    Iterator<PropertySource<?>> it = env.getPropertySources().iterator();
    while (it.hasNext()) {
      PropertySource<?> next = it.next();
      if (!(next instanceof EnumerablePropertySource)) continue;
      EnumerablePropertySource source = (EnumerablePropertySource) next;
      for (String name : source.getPropertyNames()) {
        if (name.startsWith("zipkin")) properties.put(name, source.getProperty(name));
      }
    }
    return properties;
  }

  /**
   * This holds only serializable state, in this case properties used to re-construct the zipkin
   * storage component later.
   */
  static abstract class AutoConfigurationStorageConsumer extends StorageConsumer {
    final Properties properties;

    AutoConfigurationStorageConsumer(Properties properties) {
      this.properties = properties;
    }

    @Override protected StorageComponent tryCompute() {
      AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
      PropertiesPropertySource source = new PropertiesPropertySource("consumer", properties);
      context.getEnvironment().getPropertySources().addLast(source);

      context.register(PropertyPlaceholderAutoConfiguration.class);
      registerAutoConfiguration(context);
      context.refresh();

      return context.getBean(StorageComponent.class);
    }

    abstract void registerAutoConfiguration(AnnotationConfigApplicationContext context);
  }
}


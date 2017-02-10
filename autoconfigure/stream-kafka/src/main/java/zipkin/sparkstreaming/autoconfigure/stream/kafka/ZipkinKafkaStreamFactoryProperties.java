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

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.stream.kafka.KafkaStreamFactory;
import zipkin.sparkstreaming.stream.kafka.ZookeeperBootstrapServers;

@ConfigurationProperties("zipkin.sparkstreaming.stream.kafka")
public class ZipkinKafkaStreamFactoryProperties {
  private String topic;
  private List<String> bootstrapServers;
  private Zookeeper zookeeper = new Zookeeper();

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = emptyToNull(topic);
  }

  public List<String> getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(List<String> bootstrapServers) {
    if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
      this.bootstrapServers = bootstrapServers;
    }
  }

  public Zookeeper getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(Zookeeper zookeeper) {
    if (zookeeper != null) this.zookeeper = zookeeper;
  }

  public static class Zookeeper {
    private List<String> connectServers;
    private String connectSuffix;
    private Integer sessionTimeout;

    public List<String> getConnectServers() {
      return connectServers;
    }

    public void setConnectServers(List<String> connectServers) {
      if (connectServers != null && !connectServers.isEmpty()) {
        this.connectServers = connectServers;
      }
    }

    public String getConnectSuffix() {
      return connectSuffix;
    }

    public void setConnectSuffix(String connectSuffix) {
      this.connectSuffix = emptyToNull(connectSuffix);
    }

    public Integer getSessionTimeout() {
      return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
      if (sessionTimeout > 0) this.sessionTimeout = sessionTimeout;
    }
  }

  KafkaStreamFactory.Builder toBuilder() {
    KafkaStreamFactory.Builder result = KafkaStreamFactory.newBuilder();
    if (topic != null) result.topic(topic);
    if (bootstrapServers != null) result.bootstrapServers(bootstrapServers);
    if (zookeeper.getConnectServers() == null) return result;

    ZookeeperBootstrapServers.Builder supplier = ZookeeperBootstrapServers.newBuilder();
    supplier.connectServers(zookeeper.getConnectServers());
    if (zookeeper.connectSuffix != null) supplier.connectSuffix(zookeeper.connectSuffix);
    if (zookeeper.sessionTimeout != null) supplier.sessionTimeout(zookeeper.sessionTimeout);
    result.bootstrapServers(supplier.build());
    return result;
  }

  private static String emptyToNull(String s) {
    return (s != null && !s.isEmpty()) ? s : null;
  }
}

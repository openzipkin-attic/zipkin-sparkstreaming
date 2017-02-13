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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retrieves the Kafka Bootstrap Servers from ZooKeeper. */
@AutoValue
public abstract class ZookeeperBootstrapServers implements BootstrapServers {

  public static Builder newBuilder() {
    return new AutoValue_ZookeeperBootstrapServers.Builder()
        .sessionTimeout(10000);
  }

  @AutoValue.Builder
  public interface Builder {

    /**
     * Zookeeper host string. host:port pairs corresponding to a Zookeeper server with an optional
     * chroot suffix. No default
     *
     * @see ZooKeeper#ZooKeeper(String, int, Watcher)
     */
    Builder connect(String connect);

    /**
     * Zookeeper session timeout in milliseconds. Defaults to 10000
     */
    Builder sessionTimeout(int sessionTimeout);

    ZookeeperBootstrapServers build();
  }

  abstract String connect();

  abstract int sessionTimeout();

  @Override public List<String> get() {
    ZooKeeper zkClient = null;
    try {
      zkClient = new ZooKeeper(connect(), sessionTimeout(), new NoOpWatcher());
      List<String> ids = zkClient.getChildren("/brokers/ids", false);
      ObjectMapper objectMapper = new ObjectMapper();

      List<String> brokerConnections = new ArrayList<>();
      for (String id : ids) {
        brokerConnections.add(getBrokerInfo(zkClient, objectMapper, id));
      }
      return brokerConnections;
    } catch (Exception e) {
      throw new IllegalStateException("Error loading brokers from zookeeper", e);
    } finally {
      if (zkClient != null) {
        try {
          zkClient.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Builds string to create KafkaParams for Spark job
   *
   * @param zkClient ZooKeeper client with predefined configurations
   * @param om ObjectMapper used to read zkClient's children (brokers)
   * @param id broker id
   * @return "host:port" string
   */
  private String getBrokerInfo(ZooKeeper zkClient, ObjectMapper om, String id) {
    try {
      Map map = om.readValue(zkClient.getData("/brokers/ids/" + id, false, null), Map.class);
      return map.get("host") + ":" + map.get("port");
    } catch (Exception e) {
      throw new IllegalStateException("Error reading zkClient's broker id's", e);
    }
  }

  static final class NoOpWatcher implements Watcher {

    private Logger logger = LoggerFactory.getLogger(NoOpWatcher.class);

    @Override
    public void process(WatchedEvent event) {
      logger.debug(event.toString());
    }
  }

  ZookeeperBootstrapServers() {
  }
}

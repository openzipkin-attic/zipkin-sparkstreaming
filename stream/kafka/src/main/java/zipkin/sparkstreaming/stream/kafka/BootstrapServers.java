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

import java.util.List;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
 * separated). Values are in host:port syntax. No default
 *
 * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
 */
public interface BootstrapServers {
  List<String> get();
}

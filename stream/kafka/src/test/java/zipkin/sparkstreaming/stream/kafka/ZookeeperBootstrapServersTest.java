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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: actually test the code
public class ZookeeperBootstrapServersTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void buildZookeeperBootstrapServers() throws Exception {
    ZookeeperBootstrapServers servers = ZookeeperBootstrapServers.newBuilder()
        .connect("127.0.0.1:2181")
        .build();
    assertThat(servers).isNotNull();
  }

  @Test
  public void buildFailOnMissingProperties() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Missing required properties: connect");
    ZookeeperBootstrapServers servers = ZookeeperBootstrapServers.newBuilder().build();
  }
}

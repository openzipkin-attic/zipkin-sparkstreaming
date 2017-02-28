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
package zipkin.sparkstreaming.job;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.SparkStreamingJob;

@ConfigurationProperties("zipkin.sparkstreaming")
public class ZipkinSparkStreamingProperties {
  String master;
  List<String> jars;
  Map<String, String> conf = new LinkedHashMap<>();
  Long batchDuration;

  public String getMaster() {
    return master;
  }

  public void setMaster(String master) {
    this.master = "".equals(master) ? null : master;
  }

  public List<String> getJars() {
    return jars;
  }

  public void setJars(List<String> jars) {
    if (jars != null && !jars.isEmpty()) this.jars = jars;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public void setConf(Map<String, String> conf) {
    if (conf != null && !conf.isEmpty()) {
      this.conf = conf;
    }
  }

  public long getBatchDuration() {
    return batchDuration;
  }

  public void setBatchDuration(long batchDuration) {
    this.batchDuration = batchDuration;
  }

  SparkStreamingJob.Builder toBuilder() {
    SparkStreamingJob.Builder result = SparkStreamingJob.newBuilder();
    if (master != null) result.master(master);
    if (jars != null) result.jars(jars);
    if (!conf.isEmpty()) result.conf(conf);
    if (batchDuration != null) result.batchDuration(batchDuration);
    return result;
  }
}

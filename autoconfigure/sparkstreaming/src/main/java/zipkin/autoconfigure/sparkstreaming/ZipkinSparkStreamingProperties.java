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
package zipkin.autoconfigure.sparkstreaming;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.SparkStreamingJob;

@ConfigurationProperties("zipkin.sparkstreaming")
public class ZipkinSparkStreamingProperties {
  String sparkMaster;
  List<String> sparkJars;
  Map<String, String> sparkProperties;
  Long batchDuration;

  public String getSparkMaster() {
    return sparkMaster;
  }

  public void setSparkMaster(String sparkMaster) {
    this.sparkMaster = "".equals(sparkMaster) ? null : sparkMaster;
  }

  public List<String> getSparkJars() {
    return sparkJars;
  }

  public void setSparkJars(List<String> sparkJars) {
    if (sparkJars != null && !sparkJars.isEmpty()) this.sparkJars = sparkJars;
  }

  public Map<String, String> getSparkProperties() {
    return sparkProperties;
  }

  public void setSparkProperties(Map<String, String> sparkProperties) {
    if (sparkProperties != null && !sparkProperties.isEmpty()) {
      this.sparkProperties = sparkProperties;
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
    if (sparkMaster != null) result.sparkMaster(sparkMaster);
    if (sparkJars != null) result.sparkJars(sparkJars);
    if (sparkProperties != null) result.sparkProperties(sparkProperties);
    if (batchDuration != null) result.batchDuration(batchDuration);
    return result;
  }
}

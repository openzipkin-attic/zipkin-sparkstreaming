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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.SparkStreamingJob;

@ConfigurationProperties("zipkin.sparkstreaming")
public class ZipkinSparkStreamingProperties {
  String sparkMaster = "local[*]";
  String[] sparkJars;
  final Map<String, String> sparkProperties = new LinkedHashMap<>();
  long batchDuration = 10;

  ZipkinSparkStreamingProperties() {
    sparkProperties.put("spark.ui.enabled", "false");
    // avoids strange class not found bug on Logger.setLevel
    sparkProperties.put("spark.akka.logLifecycleEvents", "true");
  }

  public String getSparkMaster() {
    return sparkMaster;
  }

  public void setSparkMaster(String sparkMaster) {
    this.sparkMaster = "".equals(sparkMaster) ? null : sparkMaster;
  }

  public String[] getSparkJars() {
    return sparkJars;
  }

  public void setSparkJars(String[] sparkJars) {
    this.sparkJars = sparkJars;
  }

  public Map<String, String> getSparkProperties() {
    return sparkProperties;
  }

  public long getBatchDuration() {
    return batchDuration;
  }

  public void setBatchDuration(long batchDuration) {
    this.batchDuration = batchDuration;
  }

  SparkStreamingJob.Builder toBuilder() {
    SparkStreamingJob.Builder builder = SparkStreamingJob.newBuilder();
    System.err.println(sparkMaster);
    builder.sparkMaster(sparkMaster);
    builder.sparkJars(sparkJars);
    builder.sparkProperties(sparkProperties);
    builder.batchDuration(10, TimeUnit.SECONDS);
    return builder;
  }
}

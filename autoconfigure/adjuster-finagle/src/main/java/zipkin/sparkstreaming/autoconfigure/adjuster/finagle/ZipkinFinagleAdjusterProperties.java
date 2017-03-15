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
package zipkin.sparkstreaming.autoconfigure.adjuster.finagle;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.adjuster.finagle.FinagleAdjuster;

@ConfigurationProperties("zipkin.sparkstreaming.adjuster.finagle")
public class ZipkinFinagleAdjusterProperties {
  private boolean applyTimestampAndDuration = true;
  private boolean spanModelTimestampAndDuration = false;

  public boolean isApplyTimestampAndDuration() {
    return applyTimestampAndDuration;
  }

  public void setApplyTimestampAndDuration(boolean applyTimestampAndDuration) {
    this.applyTimestampAndDuration = applyTimestampAndDuration;
  }

  public boolean isSpanModelTimestampAndDuration() {
    return spanModelTimestampAndDuration;
  }

  public void setSpanModelTimestampAndDuration(boolean spanModelTimestampAndDuration) {
    this.spanModelTimestampAndDuration = spanModelTimestampAndDuration;
  }

  FinagleAdjuster.Builder toBuilder() {
    return FinagleAdjuster.newBuilder()
        .applyTimestampAndDuration(applyTimestampAndDuration)
        .spanModelTimestampAndDuration(spanModelTimestampAndDuration);
  }
}

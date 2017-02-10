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
package zipkin.sparkstreaming.adjuster.finagle;

import com.google.auto.value.AutoValue;
import zipkin.BinaryAnnotation;
import zipkin.Span;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.sparkstreaming.Adjuster;

/**
 * Contains adjustments that pertain to Finagle tracing. Detection is based on the binary
 * annotations named ".*finagle.version.*".
 */
@AutoValue
public abstract class FinagleAdjuster extends Adjuster {

  public static Builder newBuilder() {
    return new AutoValue_FinagleAdjuster.Builder()
        .applyTimestampAndDuration(true);
  }

  abstract boolean applyTimestampAndDuration();

  @AutoValue.Builder
  public interface Builder {
    /**
     * As of Finagle 6.41.0, tracing is always RPC in nature, but timestamp and duration are not
     * added. This backfills timestamps. Default true
     *
     * <p>The current fix is to use zipkin-finagle to report spans.
     * See https://github.com/openzipkin/zipkin-finagle/issues/10
     */
    Builder applyTimestampAndDuration(boolean applyTimestampAndDuration);

    FinagleAdjuster build();
  }

  @Override protected boolean shouldAdjust(Span span) {
    for (BinaryAnnotation b : span.binaryAnnotations) {
      if (b.key.indexOf("finagle.version") != -1) return true;
    }
    return false;
  }

  @Override protected Span adjust(Span span) {
    if (applyTimestampAndDuration()) {
      return ApplyTimestampAndDuration.apply(span);
    }
    return span;
  }

  FinagleAdjuster() {
  }
}

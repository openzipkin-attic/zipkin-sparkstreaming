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
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Span;
import zipkin.internal.Util;
import zipkin.sparkstreaming.Adjuster;

import java.util.ArrayList;
import java.util.List;

/**
 * This adjuster handles a bug in finagle memcached library where the duration of a span ends
 * up being more than the actual value because a span is submitted twice.
 *
 * The fix is to drops finagle.flush annotation for memcache spans. We look for "Hit" or "Miss"
 * binary annotations to make sure the span is coming from memcache.
 *
 * For more details see
 * https://github.com/twitter/finagle/issues/343
 *
 */
public final class FinagleIssue343Adjuster extends Adjuster{

  FinagleIssue343Adjuster() {}

  public static FinagleIssue343Adjuster create() {
    return new FinagleIssue343Adjuster();
  }

  @Override protected boolean shouldAdjust(Span span) {
    if (containsFinagleFlushAnnotation(span) && containsHitOrMissBinaryAnnotation(span)) {
      return true;
    }
    return false;
  }

  private boolean containsHitOrMissBinaryAnnotation(Span span) {
    for (BinaryAnnotation b : span.binaryAnnotations) {
      String value = new String(b.value, Util.UTF_8);
      if (value.equals("Hit") || value.equals("Miss")) {
        return true;
      }
    }
    return false;
  }

  private boolean containsFinagleFlushAnnotation(Span span) {
    for (Annotation a : span.annotations) {
      if (a.value.equals("finagle.flush")) {
        return true;
      }
    }
    return false;
  }

  @Override protected Span adjust(Span span) {
    List<Annotation> annotations = new ArrayList<>();
    for (Annotation a : span.annotations) {
      if (!a.value.equals("finagle.flush")) {
        annotations.add(a);
      }
    }
    return span.toBuilder().annotations(annotations).build();
  }
}

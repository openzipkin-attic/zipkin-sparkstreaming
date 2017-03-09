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
package zipkin.sparkstreaming;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Span;

@AutoValue
abstract class AdjustAndConsumeSpansSharingTraceId implements Serializable,
    VoidFunction<Iterator<Iterable<Span>>> {
  private static final long serialVersionUID = 0L;
  private static final Logger log = LoggerFactory.getLogger(ReadSpans.class);

  Logger log() { // Override for testing. Instance variables won't work as Logger isn't serializable
    return log;
  }

  abstract Runnable logInitializer();

  abstract List<Adjuster> adjusters();

  abstract Consumer consumer();

  @Override public void call(Iterator<Iterable<Span>> spansSharingTraceIds) {
    logInitializer().run();
    while (spansSharingTraceIds.hasNext()) {
      Iterable<Span> spansSharingTraceId = spansSharingTraceIds.next();
      for (Adjuster adjuster : adjusters()) {
        try {
          spansSharingTraceId = adjuster.adjust(spansSharingTraceId);
        } catch (RuntimeException e) {
          log().warn("unable to adjust spans: " + spansSharingTraceId + " with " + adjuster, e);
        }
      }
      consumer().accept(spansSharingTraceId);
    }
  }
}

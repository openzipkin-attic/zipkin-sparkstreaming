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

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Annotation;
import zipkin.Constants;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.MergeById;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin.Constants.CLIENT_RECV;
import static zipkin.Constants.CLIENT_SEND;
import static zipkin.Constants.SERVER_RECV;
import static zipkin.Constants.SERVER_SEND;
import static zipkin.TestObjects.APP_ENDPOINT;
import static zipkin.TestObjects.TODAY;
import static zipkin.TestObjects.WEB_ENDPOINT;

public class AdjusterTest {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void unmodifiable() {
    thrown.expect(UnsupportedOperationException.class);

    Adjuster adjuster = new Adjuster() {
    };
    Iterable<Span> adjusted = adjuster.adjust(TestObjects.TRACE);
    adjusted.iterator().remove();
  }

  @Test
  public void defaultsToPassAllWithoutAdjusting() {
    Adjuster adjuster = new Adjuster() {
    };

    assertThat(adjuster.adjust(TestObjects.TRACE))
        .containsExactlyElementsOf(TestObjects.TRACE);
  }

  @Test
  public void canOverrideEverything() {
    Span clientSide = Span.builder().traceId(10L).id(10L).name("")
        .addAnnotation(Annotation.create((TODAY + 50) * 1000, CLIENT_SEND, WEB_ENDPOINT))
        .addAnnotation(Annotation.create((TODAY + 300) * 1000, CLIENT_RECV, WEB_ENDPOINT))
        .build();
    Span serverSide = Span.builder().traceId(10L).id(10L).name("get")
        .addAnnotation(Annotation.create((TODAY + 100) * 1000, SERVER_RECV, APP_ENDPOINT))
        .addAnnotation(Annotation.create((TODAY + 250) * 1000, SERVER_SEND, APP_ENDPOINT))
        .build();

    Adjuster adjuster = new Adjuster() {
      @Override public Iterable<Span> adjust(Iterable<Span> trace) {
        // TODO remove guava conversion once https://github.com/openzipkin/zipkin/pull/1519
        return MergeById.apply(Lists.newArrayList(trace));
      }
    };

    assertThat(adjuster.adjust(asList(clientSide, serverSide)))
        .containsExactlyElementsOf(MergeById.apply(asList(clientSide, serverSide)));
  }

  @Test
  public void shouldDrop() {
    // sanity check
    assertThat(TestObjects.TRACE).hasSize(3);

    Adjuster adjuster = new Adjuster() {
      @Override protected boolean shouldDrop(Span span) {
        return TestObjects.TRACE.get(1).equals(span);
      }
    };

    assertThat(adjuster.adjust(TestObjects.TRACE))
        .containsExactly(TestObjects.TRACE.get(0), TestObjects.TRACE.get(2));
  }

  @Test
  public void adjust() {
    Adjuster adjuster = new Adjuster() {
      @Override protected Span adjust(Span span) {
        return ApplyTimestampAndDuration.apply(span);
      }
    };

    assertThat(adjuster.adjust(TestObjects.TRACE))
        .extracting(s -> s.timestamp, s -> s.duration)
        .doesNotContainNull();
  }

  @Test
  public void shouldAdjust() {
    Adjuster adjuster = new Adjuster() {
      @Override protected boolean shouldAdjust(Span span) {
        for (Annotation a : span.annotations) {
          if (a.value.equals(Constants.SERVER_RECV) && span.parentId == null) return true;
        }
        return false;
      }

      @Override protected Span adjust(Span span) {
        return ApplyTimestampAndDuration.apply(span);
      }
    };

    assertThat(adjuster.adjust(TestObjects.TRACE)).containsExactly(
        ApplyTimestampAndDuration.apply(TestObjects.TRACE.get(0)),
        TestObjects.TRACE.get(1),
        TestObjects.TRACE.get(2)
    );
  }
}

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

import org.junit.Test;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.internal.ApplyTimestampAndDuration;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class FinagleAdjusterTest {
  FinagleAdjuster adjuster = FinagleAdjuster.newBuilder().build();

  Endpoint localEndpoint =
      Endpoint.builder().serviceName("my-host").ipv4(127 << 24 | 1).port(9411).build();
  Endpoint localEndpoint0 = localEndpoint.toBuilder().port(null).build();
  // finagle often sets to the client endpoint to the same as the local endpoint
  Endpoint remoteEndpoint = localEndpoint.toBuilder().port(63840).build();

  Span serverSpan = Span.builder()
      .traceId(-6054243957716233329L)
      .name("my-span")
      .id(-3615651937927048332L)
      .parentId(-6054243957716233329L)
      .addAnnotation(Annotation.create(1442493420635000L, Constants.SERVER_RECV, localEndpoint))
      .addAnnotation(Annotation.create(1442493422680000L, Constants.SERVER_SEND, localEndpoint))
      .addBinaryAnnotation(BinaryAnnotation.create("srv/finagle.version", "6.28.0", localEndpoint0))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.SERVER_ADDR, localEndpoint))
      .addBinaryAnnotation(BinaryAnnotation.address(Constants.CLIENT_ADDR, remoteEndpoint))
      .build();

  /** Default is to apply timestamp and duration */
  @Test
  public void adjustsFinagleSpans() throws Exception {
    Iterable<Span> adjusted = adjuster.adjust(asList(serverSpan));
    assertThat(adjusted).containsExactly(ApplyTimestampAndDuration.apply(serverSpan));
  }

  @Test
  public void applyTimestampAndDuration_disabled() throws Exception {
    adjuster = FinagleAdjuster.newBuilder().applyTimestampAndDuration(false).build();
    Iterable<Span> adjusted = adjuster.adjust(asList(serverSpan));
    assertThat(adjusted).containsExactly(serverSpan);
  }

  @Test
  public void doesntAdjustNonFinagleSpans() throws Exception {
    Iterable<Span> adjusted = adjuster.adjust(TestObjects.TRACE);
    assertThat(adjusted).containsExactlyElementsOf(TestObjects.TRACE);
  }
}

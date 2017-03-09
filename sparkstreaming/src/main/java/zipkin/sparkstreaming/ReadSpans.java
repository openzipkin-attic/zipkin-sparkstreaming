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
import java.util.Collections;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Codec;
import zipkin.Span;

@AutoValue
abstract class ReadSpans implements Serializable, FlatMapFunction<byte[], Span> {
  private static final long serialVersionUID = 0L;
  private static final Logger log = LoggerFactory.getLogger(ReadSpans.class);

  abstract Runnable logInitializer();

  // In TBinaryProtocol encoding, the first byte is the TType, in a range 0-16
  // .. If the first byte isn't in that range, it isn't a thrift.
  //
  // When byte(0) == '[' (91), assume it is a list of json-encoded spans
  //
  // When byte(0) <= 16, assume it is a TBinaryProtocol-encoded thrift
  // .. When serializing a Span (Struct), the first byte will be the type of a field
  // .. When serializing a List[ThriftSpan], the first byte is the member type, TType.STRUCT(12)
  // .. As ThriftSpan has no STRUCT fields: so, if the first byte is TType.STRUCT(12), it is a list.
  @Override public Iterable<Span> call(byte[] bytes) throws Exception {
    logInitializer().run();
    if (bytes.length == 0) return Collections.emptyList();
    try {
      if (bytes[0] == '[') {
        return Codec.JSON.readSpans(bytes);
      } else {
        if (bytes[0] == 12 /* TType.STRUCT */) {
          return Codec.THRIFT.readSpans(bytes);
        } else { // historical kafka encoding of single thrift span per message
          return Collections.singletonList(Codec.THRIFT.readSpan(bytes));
        }
      }
    } catch (RuntimeException e) {
      log.warn("unable to decode spans", e);
      return Collections.emptyList();
    }
  }
}

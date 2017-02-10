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

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.spark.api.java.JavaRDDLike;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;
import zipkin.Span;

/**
 * A {@linkplain Adjuster} is applied to {@link JavaRDDLike#map} of spans that share the same
 * trace ID. Note the input is not guaranteed to be a complete trace.
 *
 * <p>This can be used for tasks including pruning data, changing span ids, backfilling service
 * names. When dropping spans regardless of their trace, it is better to use a filter.
 *
 * <p>Implementations must be serializable, which implies you need to feed them static configuration
 */
// abstract class so we can add methods later w/o breaking compat
public abstract class Adjuster implements Serializable {

  /**
   * The default implementation allows you to conditionally adjust spans, for example those with a
   * {@link BinaryAnnotation#key tag} or a common {@link Endpoint#serviceName service}.
   *
   * <p>This works like the following:
   *
   * <pre>
   *   <ul>
   *     <li>If {@link #shouldAdjust(Span)}, {@link #adjust(Span)} is applied</li>
   *     <li>Otherwise, the span is returned as-is</li>
   *   </ul>
   * </pre>
   */
  public Iterable<Span> adjust(final Iterable<Span> trace) {
    if (trace == null) throw new NullPointerException("trace was null");
    return () -> new AdjustingIterator(this, trace.iterator());
  }

  /** By default, this doesn't adjust any spans. */
  protected boolean shouldAdjust(Span span) {
    return false;
  }

  /** By default, this returns the same span. */
  protected Span adjust(Span span) {
    return span;
  }

  static final class AdjustingIterator implements Iterator<Span> {
    final Adjuster adjuster;
    final Iterator<Span> delegate;

    AdjustingIterator(Adjuster adjuster, Iterator<Span> delegate) {
      this.adjuster = adjuster;
      this.delegate = delegate;
    }

    @Override
    public final boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public final Span next() {
      if (!hasNext()) throw new NoSuchElementException();
      Span next = delegate.next();
      if (!adjuster.shouldAdjust(next)) return next;
      return adjuster.adjust(next);
    }

    @Override
    public final void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

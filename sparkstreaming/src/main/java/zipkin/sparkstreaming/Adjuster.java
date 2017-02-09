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
import zipkin.Span;
import zipkin.internal.PeekingIterator;

import static zipkin.internal.Util.checkNotNull;

/**
 * A {@linkplain Adjuster} is applied to {@link JavaRDDLike#map} of spans that share the same
 * trace ID.
 *
 * <p>This can be used for tasks including pruning data, changing span ids, backfilling service
 * names. When dropping spans regardless of their trace, it is better to use a filter.
 */
public abstract class Adjuster implements Serializable {

  /**
   * This works like the following:
   *
   * <pre>
   *   <ul>
   *     <li>If {@link #shouldDrop(Span)}, the span isn't returned in the output</li>
   *     <li>If {@link #shouldAdjust(Span)}, {@link #adjust(Span)} is applied</li>
   *     <li>Otherwise, the span is returned as-is</li>
   *   </ul>
   * </pre>
   */
  public Iterable<Span> adjust(final Iterable<Span> trace) {
    return () -> new AdjustingIterator(Adjuster.this, trace.iterator());
  }

  /** By default, this keeps all spans. */
  protected boolean shouldDrop(Span span) {
    return false;
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
    final PeekingIterator<Span> delegate;

    AdjustingIterator(Adjuster adjuster, Iterator<Span> delegate) {
      this.adjuster = checkNotNull(adjuster, "adjuster");
      this.delegate = new PeekingIterator<>(checkNotNull(delegate, "spanIterator"));
    }

    @Override
    public final boolean hasNext() {
      while (delegate.hasNext()) {
        Span next = delegate.peek();
        if (next == null || adjuster.shouldDrop(next)) {
          delegate.next(); // drop the peeked element
          continue;
        }
        return true;
      }
      return false;
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

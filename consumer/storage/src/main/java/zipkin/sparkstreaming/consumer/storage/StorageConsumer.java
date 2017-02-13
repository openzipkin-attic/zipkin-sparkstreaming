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
package zipkin.sparkstreaming.consumer.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin.Component;
import zipkin.Span;
import zipkin.internal.CallbackCaptor;
import zipkin.sparkstreaming.Consumer;
import zipkin.storage.StorageComponent;

/** A storage consumer which writes to storage on {@link #accept(Iterable)}. */
public abstract class StorageConsumer implements Consumer, Component {
  static final Logger log = Logger.getLogger(StorageConsumer.class.getName());
  transient volatile StorageComponent instance; // not serializable

  /** Subclasses should initialize this from serializable state. */
  protected abstract StorageComponent tryCompute();

  Logger log() { // Override for testing. Instance variables won't work as Logger isn't serializable
    return log;
  }

  @Override public final void accept(Iterable<Span> spansSharingId) {
    List<Span> list = asList(spansSharingId);
    if (list.isEmpty()) {
      log().fine("Input was empty");
      return;
    }

    // Blocking as it is simpler to reason with thread this way while work is in progress
    CallbackCaptor<Void> blockingCallback = new CallbackCaptor<>();
    try {
      get().asyncSpanConsumer().accept(list, blockingCallback);
      blockingCallback.get();
      log().info("Wrote " + list.size() + " spans");
    } catch (RuntimeException e) {
      Throwable toLog = e.getClass().equals(RuntimeException.class) && e.getCause() != null
          ? e.getCause() // callback captor wraps checked exceptions
          : e;
      String message = "Dropped " + list.size() + " spans: " + toLog.getMessage();

      // TODO: We are dropping vs diverting to a dead letter queue or otherwise. Do we want this?
      if (log().isLoggable(Level.WARNING)) {
        log().log(Level.WARNING, message, toLog);
      } else {
        log().warning(message);
      }
    }
  }

  final StorageComponent get() {
    StorageComponent result = instance;
    if (result == null) {
      synchronized (this) {
        result = instance;
        if (result == null) {
          instance = result = tryCompute();
        }
      }
    }
    return result;
  }

  @Override public CheckResult check() {
    return get().check();
  }

  @Override public final void close() throws IOException {
    synchronized (this) {
      if (instance != null) instance.close();
    }
  }

  static <E> List<E> asList(Iterable<E> iter) {
    List<E> list = new ArrayList<E>();
    for (E item : iter) list.add(item);
    return list;
  }
}

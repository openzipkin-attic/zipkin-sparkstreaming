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
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import slf4jtest.LogLevel;
import slf4jtest.TestLogger;
import slf4jtest.TestLoggerFactory;
import zipkin.TestObjects;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.Callback;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageConsumerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  StorageComponent storage = mock(StorageComponent.class);
  TestLogger logger = new TestLoggerFactory().getLogger("");
  StorageConsumer storageConsumer = new StorageConsumer() {
    @Override Logger log() {
      return logger;
    }

    @Override protected StorageComponent tryCompute() {
      return storage;
    }
  };

  @Test
  public void logsWhenEmpty() {
    storageConsumer.accept(Collections.emptyList());
    assertThat(logger.lines())
        .extracting("level", "text")
        .containsExactly(tuple(LogLevel.DebugLevel, "Input was empty"));
  }

  @Test
  public void acceptsTrace() {
    storage = new InMemoryStorage();
    StorageConsumer storageConsumer = new StorageConsumer() {
      @Override Logger log() {
        return logger;
      }

      @Override protected StorageComponent tryCompute() {
        return storage;
      }
    };

    storageConsumer.accept(TestObjects.TRACE);
    assertThat(logger.lines())
        .extracting("level", "text")
        .containsExactly(tuple(LogLevel.DebugLevel, "Wrote 3 spans"));

    assertThat(storage.spanStore().getRawTrace(
        TestObjects.TRACE.get(0).traceIdHigh,
        TestObjects.TRACE.get(0).traceId
    )).isEqualTo(TestObjects.TRACE);
  }

  @Test
  public void logsOnAcceptError() {
    IllegalStateException acceptException = new IllegalStateException("failed");

    AsyncSpanConsumer consumer = mock(AsyncSpanConsumer.class);
    when(storage.asyncSpanConsumer()).thenReturn(consumer);
    doAnswer(answer(c -> {
      throw acceptException;
    })).when(consumer).accept(eq(TestObjects.TRACE), any(Callback.class));

    storageConsumer.accept(TestObjects.TRACE);

    assertThat(logger.lines())
        .extracting("level", "text")
        .containsExactly(tuple(LogLevel.WarnLevel, "Dropped 3 spans: failed"));
    // TODO: test for acceptException
  }

  @Test
  public void logsOnCallbackError() {
    IllegalStateException callbackException = new IllegalStateException("failed");

    AsyncSpanConsumer consumer = mock(AsyncSpanConsumer.class);
    when(storage.asyncSpanConsumer()).thenReturn(consumer);
    doAnswer(answer(c -> c.onError(callbackException)))
        .when(consumer).accept(eq(TestObjects.TRACE), any(Callback.class));

    storageConsumer.accept(TestObjects.TRACE);

    assertThat(logger.lines())
        .extracting("level", "text")
        .containsExactly(tuple(LogLevel.WarnLevel, "Dropped 3 spans: failed"));
    // TODO: test for callbackException
  }

  @Test
  public void doesntWrapCheckedExceptionOnCallbackError() {
    IOException callbackException = new IOException("failed");

    AsyncSpanConsumer consumer = mock(AsyncSpanConsumer.class);
    when(storage.asyncSpanConsumer()).thenReturn(consumer);
    doAnswer(answer(c -> c.onError(callbackException)))
        .when(consumer).accept(eq(TestObjects.TRACE), any(Callback.class));

    storageConsumer.accept(TestObjects.TRACE);

    assertThat(logger.lines())
        .extracting("level", "text")
        .containsExactly(tuple(LogLevel.WarnLevel, "Dropped 3 spans: failed"));
    // TODO: test for callbackException
  }

  @Test(timeout = 1000L)
  public void get_memoizes() throws InterruptedException {
    AtomicInteger provisionCount = new AtomicInteger();

    StorageConsumer storageConsumer = new StorageConsumer() {
      @Override protected StorageComponent tryCompute() {
        provisionCount.incrementAndGet();
        return new InMemoryStorage();
      }
    };

    int getCount = 1000;
    CountDownLatch latch = new CountDownLatch(getCount);
    ExecutorService exec = Executors.newFixedThreadPool(10);
    for (int i = 0; i < getCount; i++) {
      exec.execute(() -> {
        storageConsumer.get();
        latch.countDown();
      });
    }
    latch.await();
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.SECONDS);

    assertThat(provisionCount.get()).isEqualTo(1);
  }

  static <T> Answer answer(Consumer<Callback<T>> onCallback) {
    return invocation -> {
      onCallback.accept((Callback) invocation.getArguments()[invocation.getArguments().length - 1]);
      return null;
    };
  }
}

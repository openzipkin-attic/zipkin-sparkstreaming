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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin.internal.Nullable;

class TestLogger extends Logger {

  @AutoValue
  static abstract class LogMessage {
    static LogMessage create(Level level, String msg, Throwable thrown) {
      return new AutoValue_TestLogger_LogMessage(level, msg, thrown);
    }

    abstract Level level();

    abstract String msg();

    @Nullable abstract Throwable thrown();
  }

  List<LogMessage> messages = new ArrayList<>();

  TestLogger() {
    super("", null);
  }

  @Override
  public void log(Level level, String msg) {
    messages.add(LogMessage.create(level, msg, null));
  }

  @Override
  public void log(Level level, String msg, Throwable thrown) {
    messages.add(LogMessage.create(level, msg, thrown));
  }
}

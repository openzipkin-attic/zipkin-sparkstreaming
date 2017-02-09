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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.Codec;
import zipkin.TestObjects;
import zipkin.autoconfigure.sparkstreaming.ZipkinSparkStreamingAutoConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinSparkStreamingJobAutoConfigurationTest {

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void close() {
    context.close();
  }

  @Test
  public void providesSparkStreaming() throws IOException {
    context = new AnnotationConfigApplicationContext();
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        ZipkinSparkStreamingAutoConfiguration.class);
    context.refresh();

    SparkStreamingJob collector = context.getBean(SparkStreamingJob.class);
    assertThat(collector).isNotNull();
    collector.close();
  }

  @Configuration
  static class DummyConfiguration {

    @Bean MessageStreamFactory messagesFactory() {
      return jsc -> {
        Queue<JavaRDD<byte[]>> rddQueue = new LinkedList<>();
        byte[] oneTrace = Codec.JSON.writeSpans(TestObjects.TRACE);
        rddQueue.add(jsc.sparkContext().parallelize(Collections.singletonList(oneTrace)));
        return jsc.queueStream(rddQueue);
      };
    }

    @Bean TraceConsumer traceConsumer() {
      return trace -> {
        System.err.println(trace);
      };
    }
  }
}

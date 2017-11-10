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
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.Codec;
import zipkin.TestObjects;
import zipkin.sparkstreaming.job.ZipkinSparkStreamingConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinSparkStreamingJobAutoConfigurationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
  SparkStreamingJob job;

  @After
  public void close() throws IOException {
    if (job != null) job.close();
    context.close();
  }

  @Test
  public void providesSparkStreaming() throws IOException {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job).isNotNull();
  }

  @Configuration
  static class DummyConfiguration {

    @Bean StreamFactory streamFactory() {
      return jsc -> {
        Queue<JavaRDD<byte[]>> rddQueue = new ArrayDeque<>();
        byte[] oneTrace = Codec.JSON.writeSpans(TestObjects.TRACE);
        rddQueue.add(jsc.sparkContext().parallelize(Collections.singletonList(oneTrace)));
        return jsc.queueStream(rddQueue);
      };
    }

    @Bean Consumer consumer() {
      return trace -> {
        System.err.println(trace);
      };
    }
  }

  @Configuration
  static class AdjusterConfiguration {

    static Adjuster one = new Adjuster() {
    };
    static Adjuster two = new Adjuster() {
    };

    @Bean Adjuster one() {
      return one;
    }

    @Bean Adjuster two() {
      return two;
    }
  }

  @Test
  public void providesAdjusters() throws IOException {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job).isNotNull();
    assertThat(job.adjusters())
        .contains(AdjusterConfiguration.one, AdjusterConfiguration.two);
  }

  @Test
  public void defaultConf() {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.conf()).containsExactly(
        entry("spark.ui.enabled", "false")
    );
  }

  @Test
  public void canOverrideConf() {
    addEnvironment(context,
        "zipkin.sparkstreaming.conf.spark.ui.enabled:" + true);
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.conf()).containsEntry(
        "spark.ui.enabled", "true"
    );
  }

  @Test
  public void canOverrideLogLevel() {
    addEnvironment(context, "zipkin.log-level:debug");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.zipkinLogLevel()).isEqualTo("debug");
  }

  /** Default is empty, which implies we lookup the current classpath. */
  @Test
  public void defaultJars() {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.jars()).isEmpty();
  }

  @Test
  public void canOverrideJars() {
    addEnvironment(context,
        "zipkin.sparkstreaming.jars:foo.jar,bar.jar");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.jars()).containsExactly(
        "foo.jar", "bar.jar"
    );
  }

  /** Default is empty, which implies we lookup the current classpath. */
  @Test
  public void defaultBatchDuration() {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.batchDuration()).isEqualTo(10_000);
  }

  @Test
  public void canOverrideBatchDuration() {
    addEnvironment(context,
        "zipkin.sparkstreaming.batch-duration:1000");
    context.register(PropertyPlaceholderAutoConfiguration.class,
        DummyConfiguration.class,
        AdjusterConfiguration.class,
        ZipkinSparkStreamingConfiguration.class);
    context.refresh();

    job = context.getBean(SparkStreamingJob.class);
    assertThat(job.batchDuration()).isEqualTo(1_000);
  }
}

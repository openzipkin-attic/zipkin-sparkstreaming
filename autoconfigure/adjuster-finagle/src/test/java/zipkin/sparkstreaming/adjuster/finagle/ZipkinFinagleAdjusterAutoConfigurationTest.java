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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin.sparkstreaming.autoconfigure.adjuster.finagle.ZipkinFinagleAdjusterAutoConfiguration;
import zipkin.sparkstreaming.autoconfigure.adjuster.finagle.ZipkinFinagleAdjusterProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class ZipkinFinagleAdjusterAutoConfigurationTest {

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void close() {
    if (context != null) context.close();
  }

  @Test
  public void doesntProvideAdjusterWhenDisabled() {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterProperties.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(FinagleAdjuster.class);
  }

  @Test
  public void providesAdjusterWhenEnabled() {
    addEnvironment(context,
        "zipkin.sparkstreaming.adjuster.finagle.enabled:" + true);
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    FinagleAdjuster adjuster = context.getBean(FinagleAdjuster.class);
    assertThat(adjuster.applyTimestampAndDuration()).isTrue();
  }

  @Test
  public void disableTimestampAndDuration() {
    addEnvironment(context,
        "zipkin.sparkstreaming.adjuster.finagle.enabled:" + true,
        "zipkin.sparkstreaming.adjuster.finagle.apply-timestamp-and-duration:" + false);
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    FinagleAdjuster adjuster = context.getBean(FinagleAdjuster.class);
    assertThat(adjuster.applyTimestampAndDuration()).isFalse();
  }

  @Test
  public void doesntProvideIssue343AdjusterWhenFinagleDisabled() {
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(FinagleIssue343Adjuster.class);
  }

  @Test
  public void doesntProvidesIssue343AdjusterWhenFinagleEnabledAndIssue343Disabled() {
    addEnvironment(context,
        "zipkin.sparkstreaming.adjuster.finagle.enabled:" + true);
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(FinagleIssue343Adjuster.class);
  }

  @Test
  public void providesIssue343Adjuster() {
    addEnvironment(context,
        "zipkin.sparkstreaming.adjuster.finagle.enabled:" + true,
        "zipkin.sparkstreaming.adjuster.finagle.adjust-issue343:" + true);
    context.register(PropertyPlaceholderAutoConfiguration.class,
        ZipkinFinagleAdjusterAutoConfiguration.class);
    context.refresh();

    FinagleIssue343Adjuster adjuster = context.getBean(FinagleIssue343Adjuster.class);
    assertThat(adjuster).isNotNull();
  }
}

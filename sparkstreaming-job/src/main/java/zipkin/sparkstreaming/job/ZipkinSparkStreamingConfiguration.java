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
package zipkin.sparkstreaming.job;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.sparkstreaming.Adjuster;
import zipkin.sparkstreaming.Consumer;
import zipkin.sparkstreaming.SparkStreamingJob;
import zipkin.sparkstreaming.StreamFactory;

@Configuration
@EnableConfigurationProperties(ZipkinSparkStreamingProperties.class)
public class ZipkinSparkStreamingConfiguration {
  final Logger log = LoggerFactory.getLogger(ZipkinSparkStreamingConfiguration.class);

  @Autowired(required = false)
  List<Adjuster> adjusters = Collections.emptyList();

  @Bean SparkStreamingJob sparkStreaming(
      ZipkinSparkStreamingProperties sparkStreaming,
      StreamFactory streamFactory,
      Consumer consumer
  ) {
    SparkStreamingJob.Builder builder = sparkStreaming.toBuilder();
    if (sparkStreaming.getMaster() != null && sparkStreaming.getJars() == null) {
      List<String> pathToJars = pathToJars(ZipkinSparkStreamingJob.class, adjusters);
      if (pathToJars != null) {
        log.info("Will distribute the following jars to the cluster: " + pathToJars);
        builder.jars(pathToJars);
      }
    }
    return builder.streamFactory(streamFactory)
        .adjusters(adjusters)
        .consumer(consumer)
        .build()
        .start();
  }

  /**
   * This assumes everything is in the uber-jar except perhaps the adjusters (which are themselves
   * self-contained jars).
   */
  static List<String> pathToJars(Class<?> entryPoint, List<Adjuster> adjusters) {
    Set<String> jars = new LinkedHashSet<>();
    jars.add(pathToJar(entryPoint));
    for (Adjuster adjuster : adjusters) {
      jars.add(pathToJar(adjuster.getClass()));
    }
    jars.remove(null);
    return jars.isEmpty() ? null : new ArrayList<>(jars);
  }

  static String pathToJar(Class<?> type) {
    URL jarFile = type.getProtectionDomain().getCodeSource().getLocation();
    try {
      return URLDecoder.decode(jarFile.getPath(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }
}


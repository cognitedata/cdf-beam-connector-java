package com.cognite.beam.io;

import com.cognite.beam.io.config.ProjectConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProjectConfigTest {
  final String api_key = RandomStringUtils.randomAlphanumeric(32);
  final String project = RandomStringUtils.randomAlphanumeric(64);
  final String host = RandomStringUtils.randomAlphabetic(64);

  @Test
  void basicBuilder() {

    ProjectConfig config = ProjectConfig.create()
        .withProject(project)
        .withApiKey(api_key)
        .withHost(host);

    assertEquals(project, config.getProject().get());
    assertEquals(api_key, config.getApiKey().get());
    assertEquals(host, config.getHost().get());
  }

  @Test
  void valueProviderBuilder() {
    ProjectConfig config = ProjectConfig.create()
        .withProject(ValueProvider.StaticValueProvider.of(project))
        .withApiKey(ValueProvider.StaticValueProvider.of(api_key))
        .withHost(ValueProvider.StaticValueProvider.of(host));

    assertEquals(project, config.getProject().get());
    assertEquals(api_key, config.getApiKey().get());
    assertEquals(host, config.getHost().get());
  }

  @Test
  void create() {
    ProjectConfig config = ProjectConfig.create()
            .withProject(ValueProvider.StaticValueProvider.of(project))
            .withApiKey(ValueProvider.StaticValueProvider.of(api_key));

    assertEquals(project, config.getProject().get());
    assertEquals(api_key, config.getApiKey().get());
  }
}
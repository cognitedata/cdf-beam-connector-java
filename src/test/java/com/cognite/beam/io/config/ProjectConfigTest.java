package com.cognite.beam.io.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class ProjectConfigTest {

    @Test
    public void testTrivialYaml() {
        String _yaml = "host: blah\nconfigured: true";
        ProjectConfig config = ProjectConfig.create().withYaml(_yaml);
        assertNotNull(config);
        assertTrue(config.isConfigured());
        assertEquals(config.getHost().get(), "blah");
    }

    @Test
    public void basicYaml() throws IOException {
        String _yaml = Files.readString(Path.of("./src/test/resources/basic-project-config.yaml"));
        ProjectConfig config = ProjectConfig.create().withYaml(_yaml);
        assertNotNull(config);
        assertTrue(config.isConfigured());
        assertEquals(config.getHost().get(), "https://api.cognitedata.com");
        assertEquals(config.getApiKey().get(), "something");
    }

    @Test
    public void completeYaml() throws IOException {
        String _yaml = Files.readString(Path.of("./src/test/resources/complete-project-config.yaml"));
        ProjectConfig config = ProjectConfig.create().withYaml(_yaml);
        assertNotNull(config);
        assertTrue(config.isConfigured());
        assertEquals(config.getHost().get(), "https://api.cognitedata.com");
        assertNull(config.getApiKey());
        assertEquals(Objects.requireNonNull(config.getClientId()).get(), "boo");
        assertEquals(Objects.requireNonNull(config.getClientSecret()).get(), "foo");
        assertNotNull(config.getClientSecretGcpSecretConfig());
        assertEquals(config.getClientSecretGcpSecretConfig().getProjectId().get(), "test-project");
        assertEquals(config.getClientSecretGcpSecretConfig().getSecretId().get(), "secret-name");
        assertEquals(config.getClientSecretGcpSecretConfig().getSecretVersion().get(), "latest");
    }
}

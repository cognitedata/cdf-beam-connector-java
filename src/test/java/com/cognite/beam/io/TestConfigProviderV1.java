package com.cognite.beam.io;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.client.config.TokenUrl;
import com.google.common.base.Strings;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestConfigProviderV1 {
    private final static String apiVersion = "v1";
    protected static ProjectConfig projectConfigApiKey;
    protected static ProjectConfig projectConfigClientCredentials;
    protected static String appIdentifier = "Beam SDK unit test";
    protected static String rawDbName = "the_best_tests";
    protected static String rawTableName = "best_table";
    protected static String deltaIdentifier = "unitTest";
    protected static String deltaTable = "timestamp.test";

    public static void init() {
        projectConfigApiKey = ProjectConfig.create()
                .withHost(getHost())
                .withApiKey(getApiKey());

        try {
            projectConfigClientCredentials = ProjectConfig.create()
                    .withHost(getHost())
                    .withProject(getProject())
                    .withClientId(getClientId())
                    .withClientSecret(getClientSecret())
                    .withTokenUrl(TokenUrl.generateAzureAdURL(getTenantId()).toString());
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static String getProject() {
        String project = System.getenv("TEST_PROJECT");

        if (Strings.isNullOrEmpty(project)) {
            project = "test";
        }

        return project;
    }

    protected static String getApiKey() {
        String apiKey = System.getenv("TEST_KEY");

        if (Strings.isNullOrEmpty(apiKey)) {
            apiKey = "test";
        }

        return apiKey;
    }

    public static String getClientId() {
        String clientId = System.getenv("TEST_CLIENT_ID");

        if (Strings.isNullOrEmpty(clientId)) {
            clientId = "default";
        }
        return clientId;
    }

    public static String getClientSecret() {
        String clientSecret = System.getenv("TEST_CLIENT_SECRET");

        if (Strings.isNullOrEmpty(clientSecret)) {
            clientSecret = "default";
        }
        return clientSecret;
    }

    public static String getTenantId() {
        String tenantId = System.getenv("TEST_TENANT_ID");

        if (Strings.isNullOrEmpty(tenantId)) {
            tenantId = "default";
        }
        return tenantId;
    }

    protected static String getHost() {
        String host = System.getenv("TEST_HOST");

        if (Strings.isNullOrEmpty(host)) {
            host = "http://localhost:4567";
        }
        return host;
    }

    protected static String getProjectConfigFileName() {
        String fileName = System.getenv("TEST_CONFIG_FILE");
        if (Strings.isNullOrEmpty(fileName)) {
            fileName = "";
        }
        return fileName;
    }

    protected static class TestFilenamePolicy extends FileBasedSink.FilenamePolicy {
        private final String baseFileName;
        private final String suffix;

        public TestFilenamePolicy(String baseFileName, String suffix) {
            this.baseFileName = baseFileName;
            this.suffix = suffix;
        }

        public ResourceId unwindowedFilename(int shardNumber,
                                             int numShards,
                                             FileBasedSink.OutputFileHints outputFileHints) {
            return FileSystems.matchNewResource(String.format(
                    baseFileName + "%03d-of-%03d-" + suffix,
                    shardNumber,
                    numShards
                    ),
                    false);

        }

        public ResourceId windowedFilename(int shardNumber,
                                           int numShards,
                                           BoundedWindow window,
                                           PaneInfo paneInfo,
                                           FileBasedSink.OutputFileHints outputFileHints) {
            String windowString = "", paneString = "";
            if (null != window) {
                windowString = windowToString(window);
            }
            if (null != paneInfo) {
                paneString = paneInfoToString(paneInfo);
            }

            return FileSystems.matchNewResource(String.format(
                    baseFileName + "-%03d-of-%03d-" + windowString + paneString + suffix,
                    shardNumber,
                    numShards
                    ),
                    false);

        }

        private String windowToString(BoundedWindow window) {
            if (window instanceof GlobalWindow) {
                return "GlobalWindow";
            }
            if (window instanceof IntervalWindow) {
                IntervalWindow iw = (IntervalWindow) window;
                return String.format("%s-%s",
                        DateTimeFormatter
                                .ofPattern("yyyy-MM-dd-HH-mm-ss")
                                .withZone(ZoneId.of("UTC"))
                                .format(Instant.ofEpochMilli(iw.start().getMillis())),
                        DateTimeFormatter
                                .ofPattern("yyyy-MM-dd-HH-mm-ss")
                                .withZone(ZoneId.of("UTC"))
                                .format(Instant.ofEpochMilli(iw.end().getMillis())));
            }
            return window.toString();
        }

        private String paneInfoToString(PaneInfo paneInfo) {
            String paneString = String.format("pane-%d", paneInfo.getIndex());
            if (paneInfo.getTiming() == PaneInfo.Timing.LATE) {
                paneString = String.format("%s-late", paneString);
            }
            if (paneInfo.isLast()) {
                paneString = String.format("%s-last", paneString);
            }
            return paneString;
        }
    }
}

package com.cognite.client;

import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.dto.LoginStatus;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the main entry point for interacting with this SDK (and Cognite Data Fusion).
 *
 * All services are exposed via this object.
 */
@AutoValue
public abstract class CogniteClient implements Serializable {
    private final static String DEFAULT_BASE_URL = "https://api.cognitedata.com";
    private final static String API_ENV_VAR = "COGNITE_API_KEY";

    private final static OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(90, TimeUnit.SECONDS)
            .readTimeout(90, TimeUnit.SECONDS)
            .writeTimeout(90, TimeUnit.SECONDS)
            .build();

    private static final int DEFAULT_CPU_MULTIPLIER = 8;
    private static ForkJoinPool executorService = new ForkJoinPool(Runtime.getRuntime().availableProcessors()
            * DEFAULT_CPU_MULTIPLIER);

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Nullable
    private String cdfProjectCache = null; // Cache attribute for the CDF project

    private static Builder builder() {
        return new AutoValue_CogniteClient.Builder()
                .setClientConfig(ClientConfig.create())
                .setBaseUrl(DEFAULT_BASE_URL);
    }

    /**
     * Returns a {@link CogniteClient} using an API key from the system's environment
     * variables (COGNITE_API_KEY) and using default settings.
     * @return the client object.
     * @throws Exception if the api key cannot be read from the system environment.
     */
    public static CogniteClient create() throws Exception {
        String apiKey = System.getenv(API_ENV_VAR);
        if (null == apiKey) {
            String errorMessage = "The environment variable " + API_ENV_VAR + " is not set. Either provide "
                    + "an api key directly to the client or set it via " + API_ENV_VAR;
            throw new Exception(errorMessage);
        }

        return CogniteClient.ofKey(apiKey);
    }

    public static CogniteClient ofKey(String apiKey) {
        Preconditions.checkArgument(null != apiKey && !apiKey.isEmpty(),
                "The api key cannot be empty.");
        return CogniteClient.builder()
                .setApiKey(apiKey)
                .build();
    }

    protected abstract Builder toBuilder();
    protected abstract String getApiKey();
    @Nullable
    protected abstract String getProject();
    protected abstract String getBaseUrl();
    public abstract ClientConfig getClientConfig();

    protected OkHttpClient getHttpClient() {
        return httpClient;
    }
    protected ForkJoinPool getExecutorService() {
        return executorService;
    }

    /**
     * Returns a {@link CogniteClient} using the specified api key.
     *
     * @param key The api key to use for interacting with Cognite Data Fusion.
     * @return the client object with the api key set.
     */
    public CogniteClient withApiKey(String key) {
        Preconditions.checkArgument(null != key && !key.isEmpty(),
                "The api key cannot be empty.");
        return toBuilder().setApiKey(key).build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified Cognite Data Fusion project / tenant.
     *
     * @param project The project / tenant to use for interacting with Cognite Data Fusion.
     * @return the client object with the project / tenant key set.
     */
    public CogniteClient withProject(String project) {
        return toBuilder().setProject(project).build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified base URL for issuing API requests.
     *
     * The base URL must follow the format {@code https://<my-host>.cognitedata.com}. The default
     * base URL is {@code https://api.cognitedata.com}
     *
     * @param baseUrl The CDF api base URL
     * @return the client object with the base URL set.
     */
    public CogniteClient withBaseUrl(String baseUrl) {
        Preconditions.checkArgument(null != baseUrl && !baseUrl.isEmpty(),
                "The base URL cannot be empty.");
        return toBuilder().setBaseUrl(baseUrl).build();
    }

    /**
     * Returns a {@link CogniteClient} using the specified configuration settings.
     *
     * @param config The {@link ClientConfig} hosting the client configuration setting.
     * @return the client object with the config applied.
     */
    public CogniteClient withClientConfig(ClientConfig config) {
        // Modify the no threads in the executor service based on the config
        LOG.info("Setting up client with {} worker threads and {} list partitions",
                config.getNoWorkers(),
                config.getNoListPartitions());
        executorService = new ForkJoinPool(config.getNoWorkers());

        return toBuilder().setClientConfig(config).build();
    }

    /**
     * Returns {@link Assets} representing the Cognite assets api endpoint.
     *
     * @return The assets api object.
     */
    public Assets assets() {
        return Assets.of(this);
    }

    /**
     * Returns {@link Events} representing the Cognite events api endpoint.
     *
     * @return The events api object.
     */
    public Events events() {
        return Events.of(this);
    }

    /**
     * Returns the services layer mirroring the Cognite Data Fusion API.
     * @return
     */
    protected ConnectorServiceV1 getConnectorService() {
        return ConnectorServiceV1.create(getClientConfig().getMaxRetries());
        // todo add executor and client spec here. Must refactor the Beam DoFns too (SDK must be added as a non-serialized variable).
    }

    /**
     * Returns a auth info for api requests
     * @return project config with auth info populated
     * @throws Exception
     */
    protected ProjectConfig buildProjectConfig() throws Exception {
        String cdfProject = null;
        if (null != getProject()) {
            // The project is explicitly defined
            cdfProject = getProject();
        } else if (null != cdfProjectCache) {
            // The project info is cached
            cdfProject = cdfProjectCache;
        } else {
            // Have to get the project via the api key
            LoginStatus loginStatus = getConnectorService()
                    .readLoginStatusByApiKey(getBaseUrl(), getApiKey());

            if (loginStatus.getProject().isEmpty()) {
                throw new Exception("Could not find the project for the api key.");
            }
            LOG.debug("Project identified for the api key. Project: {}", loginStatus.getProject());
            cdfProjectCache = loginStatus.getProject(); // Cache the result
            cdfProject = loginStatus.getProject();
        }

        return ProjectConfig.create()
                .withHost(getBaseUrl())
                .withApiKey(getApiKey())
                .withProject(cdfProject);
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setApiKey(String value);
        abstract Builder setProject(String value);
        abstract Builder setBaseUrl(String value);
        abstract Builder setClientConfig(ClientConfig value);

        abstract CogniteClient build();
    }
}

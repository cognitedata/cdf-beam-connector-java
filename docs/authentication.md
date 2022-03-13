

## Authentication

There are two authentication options:
- OpenID Connect.
- API keys.

### OpenID Connect

OpenID Connect tokens are access tokens provided by an identity provider (like Azure AD, Octa, etc.). This token
is then used to access Cognite Data Fusion. The flow can be as follows:
1) The client authenticates towards the identity provider and obtains an access token.
2) The client uses the access token from 1) when interacting with Cognite Data Fusion.

The SDK has built-in support for the client credentials authentication flow for native tokens. This is the
default flow for services (machine to machine) like extractors, data applications (transformations, etc.).

In order to use this flow, you need to register your client with the identity provider. In the case of Azure AD
this would typically be registering your client as an "app registration". Then use the client credentials (sourced
from Azure AD) as inputs to the SDK:
```java
// The auth is configured via the ProjectConfig object
ProjectConfig projectConfig = ProjectConfig.create()
        .withClientId(<clientId>)
        .withClientSecret(<clientSecret>)
        .withTokenUrl(TokenUrl.generateAzureAdURL(<azureAdTenantId>).toString())
        .withProject("myCdfProject")
        .withAuthScopes("https://api.cognitedata.com/.default", "<my-scope>");  // Optional. You can supply custom auth scopes.

// ... and then injected to the I/O transforms
PCollection<Event> readResults = pipeline
        .apply("Read events", CogniteIO.readEvents()
                .withProjectConfig(projectConfig)                       // the auth config
                .withRequestParameters(RequestParameters.create()
                        .withFilterParameter("type", "myEventType")));
```

### API keys

Authentication via API key is the legacy method of authenticating services towards Cognite Data Fusion.
You simply supply the API key when creating the client:
```java
// The auth is configured via the ProjectConfig object
ProjectConfig projectConfig = ProjectConfig.create()
        .withApiKey(<myApiKey>)
        .withProject("myCdfProject");

// ... and then injected to the I/O transforms
PCollection<Event> readResults = pipeline
        .apply("Read events", CogniteIO.readEvents()
                .withProjectConfig(projectConfig)                      // the auth config
                .withRequestParameters(RequestParameters.create()
                        .withFilterParameter("type", "myEventType")));
```

### Configuring the auth and Cognite Data Fusion tenant settings

There are three main ways of applying the auth and CDF tenant settings:
- Direct parameters in code.
- Via a configuration file.
- Combination of direct parameters and secrets in vaults.

## Direct parameters

The basic way of applying the configuration is directly via parameters. You wire in all the parameters
using the method of your choice (via env variables, config file, vault, other) and submit it directly
to the `ProjectConfig`:
```java
// Capture the config
String clientId = readFromMyConfigHandler();
String clientSecret = readFromMyConfigHandler();
String azureAdTenantId = readFromMyConfigHandler();

// The auth is configured via the ProjectConfig object
ProjectConfig projectConfig = ProjectConfig.create()
        .withClientId(clientId)
        .withClientSecret(clientSecret)
        .withTokenUrl(TokenUrl.generateAzureAdURL(azureAdTenantId).toString())
        .withProject("myCdfProject");
```

## Config file

The SDK has build-in support for supplying the `ProjectConfig` via a TOML configuration file:
```java
// Read the config file from cloud buckets or network shares (or from local file)
String configFileUri = "gs://myBucket/myFolder/myFile.toml"
        
// Supply the file URI directly to the I/O transforms
PCollection<Event> readResults = pipeline
        .apply("Read events", CogniteIO.readEvents()
                .withProjectConfigFile(configFileUri)                      // the project config from file
                .withRequestParameters(RequestParameters.create()
                .withFilterParameter("type", "myEventType")));
```

The TOML file itself should contain all the necessary config parameters:
```toml
#------------------------------------------------------------------------
# Configuration file for Cognite Data fusion authentication settings.
#
# You should choose either 1) OpenID connect client credentials
# or 2) api keys.
#------------------------------------------------------------------------
[project]
host = "https://api.cognitedata.com"
cdf_project = "my-cdf-project"

#------------------------------------------------------------------------
# OpenID connect client credentials
#------------------------------------------------------------------------
client_id = "my-client-id-from-the-identity-provider"
client_secret = "my-client-id-from-the-identity-provider"
token_url = "https://login.microsoftonline.com/<azure-ad-tenant-id>/oauth2/v2.0/token"

#------------------------------------------------------------------------
# API key
#------------------------------------------------------------------------
api_key = "my-key-from-cdf"
```

## Combination of parameters and vault

Another option is to combine parameters with a vault. Currently only GCP Secret Manager has built-in
support. 

With this pattern, you would source the non-secret parameters from your source of choice while 
the secrets are sourced from the vault:
```java
// Capture the non-secret config settings
String clientId = readFromMyConfigHandler();
String azureAdTenantId = readFromMyConfigHandler();

// Source the secret from GCP Secret Manager
GcpSecretConfig clientSecretConfig = GcpSecretConfig.of(<myGcpTenantId>, <mySecretId>);

// The auth is configured via the ProjectConfig object
ProjectConfig projectConfig = ProjectConfig.create()
        .withClientId(clientId)
        .withClientSecretFromGcpSecret(clientSecretConfig)
        .withTokenUrl(TokenUrl.generateAzureAdURL(azureAdTenantId).toString());


// For the api key scenario
// Source the secret from GCP Secret Manager
GcpSecretConfig apiKeySecretConfig = GcpSecretConfig.of(<myGcpTenantId>, <mySecretId>);

// The auth is configured via the ProjectConfig object
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(apiKeySecretConfig);
```
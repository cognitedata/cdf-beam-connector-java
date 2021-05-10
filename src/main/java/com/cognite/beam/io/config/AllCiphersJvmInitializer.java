package com.cognite.beam.io.config;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;

import java.security.Security;

/**
 * This is a custom JVM initializer that overrides potential runtime TLS restrictions.
 *
 * Google Dataflow currently (as of May, 2021) disables a set of TLS algorithms on its VMs (due to poor
 * Java 8 performance). This leads to
 * problems when interacting with other Google APIs. In particular the cloud storage API.
 *
 * The combination of Dataflow, Java 11 and Cloud Storage leads to connection errors as the client and server
 * is unable to negotiate a secure connection. By re-enabling the full suite of Java 11 TLS algorithms
 * these connections can be established.
 */
@AutoService(value = JvmInitializer.class)
public class AllCiphersJvmInitializer implements JvmInitializer {

    @Override
    public void onStartup() {
        Security.setProperty("jdk.tls.disabledAlgorithms", "");
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {}
}
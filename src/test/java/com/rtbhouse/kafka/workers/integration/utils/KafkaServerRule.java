package com.rtbhouse.kafka.workers.integration.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class KafkaServerRule implements MethodRule, TestRule {

    private final Properties kafkaProperties;
    private TestingServer zookeeperServer;
    private KafkaServerStartable server;
    private File logDir;
    private final Random random = new Random();

    public KafkaServerRule(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try (KafkaServerScope scope = new KafkaServerScope(requiresKafkaServer(description))) {
                    base.evaluate();
                }
            }
        };
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try (KafkaServerScope scope = new KafkaServerScope(requiresKafkaServer(method))) {
                    base.evaluate();
                }
            }
        };
    }

    private class KafkaServerScope implements AutoCloseable {

        public KafkaServerScope(boolean shouldStart) throws Exception {
            if (shouldStart) {
                startKafkaServer();
            }
        }

        @Override
        public void close() throws Exception {
            stopKafkaServer();
        }
    }

    private boolean requiresKafkaServer(FrameworkMethod method) {
        return method.getAnnotation(RequiresKafkaServer.class) != null
                || method.getMethod().getDeclaringClass().getAnnotation(RequiresKafkaServer.class) != null;
    }

    private boolean requiresKafkaServer(Description description) {
        return description.getAnnotation(RequiresKafkaServer.class) != null
                || description.getTestClass().getAnnotation(RequiresKafkaServer.class) != null;
    }


    private void startKafkaServer() throws Exception {
        final Properties properties = new Properties();
        properties.putAll(kafkaProperties);

        if (!properties.contains("zookeeper.connect")) {
            zookeeperServer = new TestingServer(new InstanceSpec(
                    Files.createTempDirectory("zoo" + random.nextInt()).toFile(), -1, -1, -1, true, -1), true);
            properties.put("zookeeper.connect", zookeeperServer.getConnectString());
        }

        if (!properties.contains("log.dir")) {
            logDir = Files.createTempDirectory("state" + random.nextInt()).toFile();
            properties.put("log.dir", logDir.getAbsolutePath() + "/kafka-logs");
        } else {
            logDir = new File(kafkaProperties.getProperty("log.dir"));
        }

        KafkaConfig config = new KafkaConfig(properties);

        server = new KafkaServerStartable(config);
        server.startup();
    }

    private void stopKafkaServer() throws IOException {
        if (server != null) {
            server.shutdown();
            server.awaitShutdown();
            server = null;
        }

        if (zookeeperServer != null) {
            zookeeperServer.close();

            if (zookeeperServer.getTempDirectory().exists()) {
                FileUtils.deleteRecursively(zookeeperServer.getTempDirectory());
            }
            zookeeperServer = null;
        }

        if (logDir != null) {
            FileUtils.deleteRecursively(logDir);
        }
    }

    public String getBootstrapServers() {
        return String.format("%s:%s",
                Optional.ofNullable(kafkaProperties.get("host.name")).orElseThrow(),
                Optional.ofNullable(kafkaProperties.get("port")).orElseThrow());
    }

    public File getLogDir() {
        return logDir;
    }

}

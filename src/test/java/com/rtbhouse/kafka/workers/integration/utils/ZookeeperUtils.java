package com.rtbhouse.kafka.workers.integration.utils;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

public final class ZookeeperUtils {

    private static final int SESSION_TIMEOUT_MS = 60000;
    private static final int CONNECTION_TIMEOUT_MS = 60000;

    private ZookeeperUtils() {
    }

    public static void createTopics(String zookeeperUrl, int partitions, int replicas, String... topicNames) throws InterruptedException {

        ZkClient zkClient = ZkUtils.createZkClient(zookeeperUrl, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
        ZkConnection zkConnection = new ZkConnection(zookeeperUrl);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);

        for (String topicName : topicNames) {
            AdminUtils.createTopic(zkUtils, topicName, partitions, replicas, new Properties(), RackAwareMode.Enforced$.MODULE$);
        }

        zkUtils.close();
        zkConnection.close();
        zkClient.close();
    }

}

package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class TestBaseCurator {
    public static volatile TestingServer testingServer;
    public static CuratorFramework client;

    @BeforeClass
    public static void start() throws Exception {
        testingServer = new TestingServer(3181, new File(String.format("target/test_zk_data/%s/", System.nanoTime())));

        client = CuratorFrameworkFactory.builder().connectString(testingServer.getConnectString())
                .retryPolicy(new RetryOneTime(3000))
                .namespace(String.format("test%s", System.nanoTime()))
                .build();
        client.start();
        client.blockUntilConnected();
        System.out.println("连接开启");
    }
    @AfterClass
    public static void close() throws IOException {
        client.close();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                try {
                    testingServer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println("连接结束");
    }

    public static void print(Object object) {
        System.out.println("Print start ===");
        if (object == null) {
            System.out.println("null");
        } else if (object instanceof Map) {
            ((Map) object).forEach((key, value) -> {
                System.out.println("Key:" + key.toString() + " = " + value.toString());
            });
        } else if (object instanceof List || object instanceof Collection) {
            for (Object obj : (Collection)object) {
                System.out.println(obj.toString());
            }
        } else {
            System.out.println(object.toString());
        }
        System.out.println("Print end ===");
    }
}

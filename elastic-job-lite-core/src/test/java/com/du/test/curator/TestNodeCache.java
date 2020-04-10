package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestNodeCache {
    private static volatile TestingServer testingServer;
    private static CuratorFramework client;

    @BeforeClass
    public static void start() throws Exception {
        testingServer = new TestingServer(3181, new File(String.format("target/test_zk_data/%s/", System.nanoTime())));

        client = CuratorFrameworkFactory.builder().connectString(testingServer.getConnectString())
                .retryPolicy(new RetryOneTime(3000))
                .namespace("test")
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

    /**
     *
     * @throws Exception
     */
    @Test
    public void trest4Node() throws Exception {
        System.out.println("====");
        NodeCache nodeCache = new NodeCache(client, "/node1");
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData currentData = nodeCache.getCurrentData();
                if (currentData == null) {
                    System.out.println("Node changed DELETED");
                } else {
                    System.out.println("Node changed " + new String(currentData.getData()));
                }

            }
        });
        nodeCache.start();

        Stat stat = client.checkExists().forPath("/node1");
        System.out.println("CheckExists " + stat);

        client.create().creatingParentsIfNeeded().forPath("/node1", "first".getBytes());
        Thread.sleep(300);

        client.setData().forPath("/node1", "second".getBytes());
        Thread.sleep(300);

        List<String> strings = client.getChildren().forPath("/");

        for (String str : strings) {
            System.out.println(str);
        }

        client.delete().deletingChildrenIfNeeded().forPath("/node1");
        Thread.sleep(300);

        nodeCache.close();
    }

}

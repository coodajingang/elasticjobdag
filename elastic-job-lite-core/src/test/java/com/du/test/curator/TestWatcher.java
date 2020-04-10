package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

public class TestWatcher extends TestBaseCurator {
    /**
     * usingWatcher 只触发一次
     * @throws Exception
     */
    @Test
    public void test4watcher() throws Exception {

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("WatchedEvent: " + watchedEvent.toString());
            }
        };
        BackgroundCallback callback = new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("BackCallBack: " + event.toString());
            }
        };
        client.getData().usingWatcher(watcher).inBackground(callback).forPath("/test");

        Thread.sleep(1000);
        client.create().forPath("/test", "test".getBytes());
        Thread.sleep(1000);
        client.setData().forPath("/test", "ttt2".getBytes());
        Thread.sleep(1000);
        client.create().forPath("/test/t1", "t1".getBytes());
        Thread.sleep(1000);
        client.delete().forPath("/test/t1");
        Thread.sleep(1000);
        client.delete().forPath("/test");
        Thread.sleep(2000);
    }
}

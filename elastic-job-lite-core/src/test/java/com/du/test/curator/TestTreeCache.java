package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.junit.Test;

import java.util.Map;

public class TestTreeCache extends TestBaseCurator {

    /**
     * 不仅关注子节点还关注本节点 ， 是NodeCache 和PathChildrenCache的合体；
     * INITIALIZED NODE_ADDED NODE_UPDATED NODE_REMOVED
     * @throws Exception
     */
    @Test
    public void test4TreeCache() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/tree/test");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("Event: " + event.getType() + " Data: " + (event.getData() == null ? "null" : event.getData().toString()));
            }
        });

        treeCache.start();

        Thread.sleep(1000);
        ChildData currentData = treeCache.getCurrentData("/tree/test");
        System.out.println("cache data: " + (currentData == null ? "null" : currentData.toString()));

        if (client.checkExists().forPath("/tree/test") != null) {
            byte[] bytes = client.getData().forPath("/tree/test");
            System.out.println("zk data: " + (bytes == null ? "null" : new String(bytes)));
        } else {
            System.out.println("cretea path ");
            client.create().creatingParentContainersIfNeeded().forPath("/tree/test", "test".getBytes());
        }

        Thread.sleep(100);
        currentData = treeCache.getCurrentData("/tree/test");
        System.out.println("cache data: " + (currentData == null ? "null" : currentData.toString()));
        byte[] bytes = client.getData().forPath("/tree/test");
        System.out.println("zk data: " + (bytes == null ? "null" : new String(bytes)));

        Thread.sleep(100);
        System.out.println("Should create child not in the paht trigger Event ?");
        String s = client.create().forPath("/tree/node1", "node1".getBytes());
        Thread.sleep(100);

        System.out.println("Should create child in the path triggerEvent?");
        client.create().forPath("/tree/test/t1", "t1".getBytes());
        client.create().forPath("/tree/test/t2", "t2".getBytes());
        client.create().forPath("/tree/test/t1/t11", "t11".getBytes());
        Thread.sleep(100);

        Map<String, ChildData> currentChildren = treeCache.getCurrentChildren("/tree/test");
        print(currentChildren);

        client.setData().forPath("/tree/test", "ttt2".getBytes());
        client.setData().forPath("/tree", "ttt3".getBytes());

        client.setData().forPath("/tree/test/t1", "x2".getBytes());
        Thread.sleep(100);

        client.setData().forPath("/tree/test/t1/t11", "x11".getBytes());
        Thread.sleep(100);
        client.delete().forPath("/tree/test/t2");
        Thread.sleep(100);
        client.delete().deletingChildrenIfNeeded().forPath("/tree/test/t1");
        Thread.sleep(100);
        client.delete().deletingChildrenIfNeeded().forPath("/tree/test");
        Thread.sleep(100);
        currentChildren = treeCache.getCurrentChildren("/tree/test");
        print(currentChildren);

        treeCache.close();
    }

}

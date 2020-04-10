package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.List;

public class TestPathCache extends TestBaseCurator{

    /**
     * 1. PathCache 是监控一个路径下的子节点的数据；
     * 2. PathCache 缓存的数据可能与zk不一致，使用时需要注意；
     * 3. 只有路径下的child会触发事件： CHILD_ADDED CHILD_UPDATED CHILD_REMOVED
     * 4. 当cacheData 为false时，getData返回null
     * @throws Exception
     */
    @Test
    public void test4PathCache () throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/example/cache", false);
        pathChildrenCache.start();

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("Event: " + event.getType() + " Data: " + event.getData().getPath() + " = " + (event.getData().getData() == null ? "null" : new String(event.getData().getData())));
            }
        });



        if (client.checkExists().forPath("/example") != null) {
            System.out.println("删除");
            client.delete().deletingChildrenIfNeeded().forPath("/example");
        }

        //client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/example", "exa".getBytes());
        Thread.sleep(10);
        //client.create().creatingParentContainersIfNeeded().forPath("/example/cache", "cac".getBytes());
        Thread.sleep(10);
        client.create().creatingParentContainersIfNeeded().forPath("/example/cache/te1", "te1".getBytes());
        Thread.sleep(10);
        client.create().creatingParentContainersIfNeeded().forPath("/example/cache/te2", "te2".getBytes());
        Thread.sleep(10);
        client.create().creatingParentContainersIfNeeded().forPath("/example/cache/te3", "te3".getBytes());
        Thread.sleep(10);
        client.setData().forPath("/example", "exa2".getBytes());
        Thread.sleep(10);
        client.setData().forPath("/example/cache", "cad".getBytes());
        Thread.sleep(10);
        client.setData().forPath("/example/cache/te3", "ee3".getBytes());
        Thread.sleep(10);
        List<ChildData> currentData = pathChildrenCache.getCurrentData();
        currentData.forEach(data -> {
            System.out.println(data.getPath() + " == " + (data.getData()==null ? "null" : new String(data.getData())));
        });

        Thread.sleep(3000);
        client.delete().forPath("/example/cache/te3");
        Thread.sleep(50);
        client.delete().deletingChildrenIfNeeded().forPath("/example");
        Thread.sleep(50);

        System.out.println("删除后 读取数据");
        currentData = pathChildrenCache.getCurrentData();
        currentData.forEach(data -> {
            System.out.println(data.getPath() + " == " + data.getData()==null ? "null" : new String(data.getData()));
        });

        pathChildrenCache.close();
    }
}

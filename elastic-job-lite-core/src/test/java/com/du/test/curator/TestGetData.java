package com.du.test.curator;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.util.List;

public class TestGetData extends TestBaseCurator{

    @Test
    public void test4getChilder() throws Exception {
        client.create().forPath("/test", "node".getBytes());
        client.create().forPath("/test/node1", "node111".getBytes());
        client.create().forPath("/test/node2", "node222".getBytes());
        client.create().forPath("/test/node3", "node323".getBytes());

        List<java.lang.String> strings = client.getChildren().forPath("/test");
        strings.forEach(System.out::println);

        byte[] bytes = client.getData().forPath("/test/" + strings.get(1));
        System.out.println(new String(bytes, Charsets.UTF_8));
    }
}

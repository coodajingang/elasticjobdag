package com.du.test.curator;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.zookeeper.server.quorum.Leader;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestLeaderLatch extends TestBaseCurator{

    /**
     * LeaderLatch 基于zk的临时顺序节点进行选举；
     * 它通过close 来放弃leader身份 并推出参与选举；
     * 成为leader后会一直保持，直到调用close释放；释放后不能再参加选举；
     * 非公平选举；
     * 适用于主从切换场景；
     *
     * @throws Exception
     */
    @Test
    public void test4latch() throws Exception {
        String path = "/latch-";
        List<LeaderLatch> latchList = IntStream.range(1, 10)
                .parallel()
                .mapToObj(d -> new LeaderLatch(client, path, "lat" + d))
                .collect(Collectors.toList());
        latchList.parallelStream()
                .forEach(latch -> {
                    try {
                        // 通过监听器获取是否leader事件 ，进行响应处理；
                        latch.addListener(new LeaderLatchListener() {
                            @Override
                            public void isLeader() {
                                System.out.println("EventisLeader: " + latch.getId());
                            }

                            @Override
                            public void notLeader() {
                                System.out.println("EventnotLeader: " + latch.getId());
                            }
                        });
                        latch.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

        TimeUnit.SECONDS.sleep(2);

        Iterator<LeaderLatch> iterator = latchList.iterator();
        while (iterator.hasNext()) {
            LeaderLatch next = iterator.next();
            if (next.hasLeadership()) {
                System.out.println(next.getId() + " is leader");
                next.close();
                iterator.remove();
            }
        }

        TimeUnit.SECONDS.sleep(2);

        latchList.stream().filter(d -> d.hasLeadership()).forEach(d -> System.out.println(d.getId() + " hasLeadership"));

        // 获取当前的leader
        Participant leader = latchList.get(0).getLeader();
        System.out.println("Leader is :" + leader.toString());


        TimeUnit.SECONDS.sleep(10);

        latchList.stream().forEach(latch -> {
            try {
                System.out.println("Close: "+ latch.getId());
                latch.close();
                TimeUnit.SECONDS.sleep(1);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        });

    }
}

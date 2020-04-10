package com.du.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestLeaderElection extends TestBaseCurator{

    /**
     * 可重新参与选举；
     * 获取leadership后执行takeLeadership() ,方法执行完成后自动推出leadership ； 通过设置 autoRequeue() 来自动参与排队，重新获取leadership ；
     *
     *
     * @throws InterruptedException
     */
    @Test
    public void test4Election() throws Exception {
        String path = "/selector";

        List<SelectWrapper> collect = IntStream.rangeClosed(1, 5).parallel().mapToObj(d -> new SelectWrapper(path, "Client-" + d, client))
                .collect(Collectors.toList());
        collect.stream().parallel().forEach(s -> s.start());

        System.out.println("Selector 已经启动");

        TimeUnit.SECONDS.sleep(30);

        Participant leader = collect.get(0).getSelector().getLeader();
        System.out.println("当前leader" + leader.toString());

        System.out.println(collect.get(0).getClient() + " is requeue ?" + collect.get(0).getSelector().requeue());



        System.out.println("Selector 开始关闭");
        collect.parallelStream().forEach(s -> s.close());



    }

    public class SelectWrapper extends LeaderSelectorListenerAdapter {

        private String path;
        private String id;
        private CuratorFramework client;
        private LeaderSelector selector;
        private AtomicInteger leaderCount;

        public SelectWrapper(String path, String id, CuratorFramework client) {
            this.path = path;
            this.id = id;
            this.client = client;
            this.selector = new LeaderSelector(client, path, this);
            this.leaderCount = new AtomicInteger();

            // 自动重新排队 ，否则只会执行一次
            this.selector.autoRequeue();
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            System.out.println("Take Leader ship :" + this.id +  " , 成为leader的次数:" + leaderCount.getAndIncrement());
            TimeUnit.SECONDS.sleep(2);
            System.out.println("Quit Leader " + this.id);
        }

        public void start() {
            this.selector.start();
        }
        public void close() {
            this.selector.close();
        }
        public LeaderSelector getSelector() {
            return selector;
        }

        public void setSelector(LeaderSelector selector) {
            this.selector = selector;
        }

        public AtomicInteger getLeaderCount() {
            return leaderCount;
        }

        public void setLeaderCount(AtomicInteger leaderCount) {
            this.leaderCount = leaderCount;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public CuratorFramework getClient() {
            return client;
        }

        public void setClient(CuratorFramework client) {
            this.client = client;
        }
    }


}

package com.du.test.dag;

import io.elasticjob.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import io.elasticjob.lite.executor.ShardingContexts;

public class TestEljobOnceListener extends AbstractDistributeOnceElasticJobListener {
    public TestEljobOnceListener(long startedTimeoutMilliseconds, long completedTimeoutMilliseconds) {
        super(startedTimeoutMilliseconds, completedTimeoutMilliseconds);
    }

    public TestEljobOnceListener() {
        super(1l, 1l);
    }

    @Override
    public void doBeforeJobExecutedAtLastStarted(ShardingContexts shardingContexts) {
        System.out.println("Job Listener BEFORE ONCE: " + shardingContexts.getJobName() );
        System.out.println("=BEFORE==ONCE===" + shardingContexts.toString());

    }

    @Override
    public void doAfterJobExecutedAtLastCompleted(ShardingContexts shardingContexts) {
        System.out.println("Job Listener AFTER ONCE: " + shardingContexts.getJobName() );
        System.out.println("=AFTER==ONCE===" + shardingContexts.toString());
    }
}

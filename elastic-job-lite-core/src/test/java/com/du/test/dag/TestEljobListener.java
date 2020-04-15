package com.du.test.dag;

import io.elasticjob.lite.api.listener.ElasticJobListener;
import io.elasticjob.lite.executor.ShardingContexts;

public class TestEljobListener implements ElasticJobListener {
    @Override
    public void beforeJobExecuted(ShardingContexts shardingContexts) {
        System.out.println("Job Listener BEFORE: " + shardingContexts.getJobName() );
        System.out.println("=BEFORE=====" + shardingContexts.toString());
    }

    @Override
    public void afterJobExecuted(ShardingContexts shardingContexts) {
        System.out.println("Job Listener AFTER: " + shardingContexts.getJobName() );
        System.out.println("=AFTER=====" + shardingContexts.toString());
    }
}

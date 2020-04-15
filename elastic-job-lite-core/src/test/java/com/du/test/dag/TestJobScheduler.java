package com.du.test.dag;

import io.elasticjob.lite.api.JobScheduler;
import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.LiteJobConfiguration;
import io.elasticjob.lite.config.simple.SimpleJobConfiguration;
import io.elasticjob.lite.dag.DagNodeStorage;
import io.elasticjob.lite.dag.JobDagConfig;
import io.elasticjob.lite.fixture.TestSimpleJob;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class TestJobScheduler extends TestBaseRegCenter {
    private static String groupName = "groupName";
    private static String jobName = "joba";

    @Test
    public void test4sss() throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(zkRegCenter, genSimpleConfig(), new TestEljobListener(), new TestEljobOnceListener());

        scheduler.init();

        TimeUnit.SECONDS.sleep(50);

        DagNodeStorage dagNodeStorage = new DagNodeStorage(zkRegCenter, groupName, jobName);
        System.out.println(dagNodeStorage.currentDagStates());
        System.out.println(dagNodeStorage.getJobDenpendencies());
    }

    private LiteJobConfiguration genSimpleConfig() {
        JobCoreConfiguration coreConfiguration = JobCoreConfiguration.newBuilder(jobName, "0,10,20,30,40,50 * * * * ?", 10).failover(true)
                .description("This is a new job")
                .misfire(true)
                .shardingItemParameters("0=A,1=B")
                .jobProperties("name", "value")
                .jobProperties("sec", "1234")
                .jobDagProperties(new JobDagConfig(groupName, "self","com.lite.default.retrypocliy" , true, false))
                .build();

        SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(coreConfiguration, TestSimpleJob.class.getCanonicalName());

        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(simpleJobConfiguration)
                .disabled(false)
                .overwrite(true)
                .monitorPort(3030)
                .reconcileIntervalMinutes(1000)
                .build();
        return liteJobConfiguration;
    }
}

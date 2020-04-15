package com.du.test.dag;

import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.LiteJobConfiguration;
import io.elasticjob.lite.config.simple.SimpleJobConfiguration;
import io.elasticjob.lite.dag.DefaultJobDagRetryPolicy;
import io.elasticjob.lite.dag.JobDagConfig;
import io.elasticjob.lite.fixture.TestSimpleJob;
import io.elasticjob.lite.internal.config.LiteJobConfigurationGsonFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class TestDagConfig {

    @Test
    public void test4DagConfig() {
        JobDagConfig config = new JobDagConfig("groupName", "job1,job2", "com.lite.default.retrypocliy", true, false);

        System.out.println(config.toString());
    }


    @Test
    public void test4CoreConfiguration() {
        JobCoreConfiguration coreConfiguration = JobCoreConfiguration.newBuilder("j1", "* * 1/1 * * ?", 10).failover(true)
                .description("This is a new job")
                .misfire(true)
                .shardingItemParameters("0=A,1=B")
                .jobProperties("name", "value")
                .jobProperties("sec", "1234")
                .jobDagProperties(new JobDagConfig("groupName", "job1,job2", "com.lite.default.retrypocliy", true, false))
                .build();

        JobDagConfig jobDagConfig = coreConfiguration.getJobDagConfig();
        System.out.println(jobDagConfig.toString());
    }

    private static String gson = "{\"jobName\":\"j1\",\"jobClass\":\"io.elasticjob.lite.fixture.TestSimpleJob\",\"jobType\":\"SIMPLE\",\"cron\":\"* * 1/1 * * ?\",\"shardingTotalCount\":10,\"shardingItemParameters\":\"0\\u003dA,1\\u003dB\",\"jobParameter\":\"\",\"failover\":true,\"misfire\":true,\"description\":\"This is a new job\",\"jobProperties\":{\"job_exception_handler\":\"io.elasticjob.lite.executor.handler.impl.DefaultJobExceptionHandler\",\"executor_service_handler\":\"io.elasticjob.lite.executor.handler.impl.DefaultExecutorServiceHandler\"},\"monitorExecution\":true,\"maxTimeDiffSeconds\":-1,\"monitorPort\":3030,\"jobShardingStrategyClass\":\"\",\"reconcileIntervalMinutes\":1000,\"disabled\":false,\"overwrite\":true}";

    private static String gsonWithDag = "{\"jobName\":\"j1\",\"jobClass\":\"io.elasticjob.lite.fixture.TestSimpleJob\",\"jobType\":\"SIMPLE\",\"cron\":\"* * 1/1 * * ?\",\"shardingTotalCount\":10,\"shardingItemParameters\":\"0\\u003dA,1\\u003dB\",\"jobParameter\":\"\",\"failover\":true,\"misfire\":true,\"description\":\"This is a new job\",\"jobProperties\":{\"job_exception_handler\":\"io.elasticjob.lite.executor.handler.impl.DefaultJobExceptionHandler\",\"executor_service_handler\":\"io.elasticjob.lite.executor.handler.impl.DefaultExecutorServiceHandler\"},\"jobDagConfig\":{\"dagGroup\":\"groupName\",\"dagDependencies\":\"job1,job2\",\"retryClass\":\"com.lite.default.retrypocliy\",\"dagRunAlone\":true,\"dagSkipWhenFail\":false},\"monitorExecution\":true,\"maxTimeDiffSeconds\":-1,\"monitorPort\":3030,\"jobShardingStrategyClass\":\"\",\"reconcileIntervalMinutes\":1000,\"disabled\":false,\"overwrite\":true}";
    @Test
    public void test4LiteJobConfig() {
        JobCoreConfiguration coreConfiguration = JobCoreConfiguration.newBuilder("j1", "* * 1/1 * * ?", 10).failover(true)
                .description("This is a new job")
                .misfire(true)
                .shardingItemParameters("0=A,1=B")
                .jobProperties("name", "value")
                .jobProperties("sec", "1234")
                .jobDagProperties(new JobDagConfig("groupName", "job1,job2","com.lite.default.retrypocliy" , true, false))
                .build();

        SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(coreConfiguration, TestSimpleJob.class.getCanonicalName());

        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(simpleJobConfiguration)
                .disabled(false)
                .overwrite(true)
                .monitorPort(3030)
                .reconcileIntervalMinutes(1000)
                .build();

        System.out.println(liteJobConfiguration);

        JobDagConfig jobDagConfig = liteJobConfiguration.getTypeConfig().getCoreConfig().getJobDagConfig();
        System.out.println(jobDagConfig);

        String s = LiteJobConfigurationGsonFactory.toJson(liteJobConfiguration);
        System.out.println(s);

    }

    @Test
    public void test4litejobfromjson() {
        LiteJobConfiguration liteJobConfiguration = LiteJobConfigurationGsonFactory.fromJson(gson);
        System.out.println(liteJobConfiguration.toString());
        System.out.println(liteJobConfiguration.getJobName());
        System.out.println(liteJobConfiguration.getTypeConfig().toString());
        assertTrue(liteJobConfiguration.getTypeConfig().getCoreConfig().getJobDagConfig() == null);

        String string = LiteJobConfigurationGsonFactory.toJson(liteJobConfiguration);
        assertThat(string, is(gson));
    }


    @Test
    public void test4litejobfromjsonwithdag() {
        LiteJobConfiguration liteJobConfiguration = LiteJobConfigurationGsonFactory.fromJson(gsonWithDag);
        System.out.println(liteJobConfiguration.toString());
        System.out.println(liteJobConfiguration.getJobName());
        System.out.println(liteJobConfiguration.getTypeConfig().toString());
        assertTrue(liteJobConfiguration.getTypeConfig().getCoreConfig().getJobDagConfig() != null);
        System.out.println(liteJobConfiguration.getTypeConfig().getCoreConfig().getJobDagConfig());
        String string = LiteJobConfigurationGsonFactory.toJson(liteJobConfiguration);
        assertThat(string, is(gsonWithDag));
    }
}

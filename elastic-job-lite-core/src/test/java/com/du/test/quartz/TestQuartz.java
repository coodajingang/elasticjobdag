package com.du.test.quartz;

import io.elasticjob.lite.internal.schedule.JobShutdownHookPlugin;
import lombok.ToString;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class TestQuartz {

    @Test
    public void testSimeTrigger() throws InterruptedException, SchedulerException {
        Scheduler scheduler = getScheduler();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1")
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(50))
                .build();
        JobDetail job = createJobDetail();
        scheduler.scheduleJob(job, trigger);

        scheduler.start();

        Thread.sleep(10* 1000);

        System.out.println("Shut down ");

        scheduler.shutdown(true);

        Thread.sleep(10 * 1000);
    }

    @Test
    public void testcornTrigger() throws SchedulerException, InterruptedException {
        Scheduler scheduler = getScheduler();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1")
                .withSchedule(CronScheduleBuilder.cronSchedule("*/2 * * * * ?").withMisfireHandlingInstructionDoNothing())
                .build();
        JobDetail job = createJobDetail();
        scheduler.scheduleJob(job, trigger);

        scheduler.start();

        Thread.sleep(10* 1000);

        if (scheduler.isStarted()) {
            System.out.println("change pause ");
            // 暂定调度， 但是正在执行的作业不还会正常执行 ；
            scheduler.pauseAll();
        }

        Thread.sleep(10* 1000);
        System.out.println("now start scheduler again ");
        System.out.println(scheduler.isStarted());
        scheduler.resumeAll();
        Thread.sleep(10* 1000);

        System.out.println("Trigger now ");
        // 立即触发job ，不关配置的trigger ，但是nextfireTime 是空的；
        scheduler.triggerJob(job.getKey());

        Thread.sleep(2 * 1000);
        System.out.println("Shut down ");

        scheduler.shutdown(true);

        Thread.sleep(30 * 1000);

    }


    private JobDetail createJobDetail() {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put("data1", "value1");
        dataMap.put("data2", 33l);
        JobDetail detail = JobBuilder.newJob(TestJob.class)
                .withIdentity("job1", "group1").withDescription("This is a testjob1")
                .usingJobData(dataMap)
                .build();
        return detail;
    }

    private Scheduler getScheduler() throws SchedulerException {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        factory.initialize(getBaseQuartzProperties());
        return factory.getScheduler();
    }

    private Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "2");
        result.put("org.quartz.scheduler.instanceName", "q-1");
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }
}

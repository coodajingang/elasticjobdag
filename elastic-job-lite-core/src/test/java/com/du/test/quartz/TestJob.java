package com.du.test.quartz;

import org.apache.commons.lang.math.RandomUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Random;

@DisallowConcurrentExecution
public class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String name = Thread.currentThread().getName();
        long id = Thread.currentThread().getId();

        System.out.println("Test Job : Thread===" + id + " " + name + " Running");
        System.out.println("Test Job : jobInstance===" + jobExecutionContext.getFireInstanceId()
                + " " + jobExecutionContext.getJobDetail().toString() + " Running");
        Date fireDate = jobExecutionContext.getFireTime();
        Date nextFireDate = jobExecutionContext.getNextFireTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime fire = fireDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
//        LocalDateTime nextFire = nextFireDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
//        System.out.println("Test job " + fire.format(formatter) + " Next:" + nextFire.format(formatter));
        for (int i = 0; i < 99999999; i++) {
            double x = Math.acos(i * new Random().nextDouble());
            try {
                //Thread.currentThread().sleep(700);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Test job: end of " + fire.format(formatter));
    }
}

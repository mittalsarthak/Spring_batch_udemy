package com.learn.sbb.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HwJobExecutionListener implements JobExecutionListener {
    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before Starting the job: " + jobExecution.getJobInstance().getJobName());
        System.out.println("Before Starting the job: " + jobExecution.getExecutionContext().toString());
        jobExecution.getExecutionContext().put("my name", "sarthak");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After Starting the job: " + jobExecution.getExecutionContext().toString());
    }
}

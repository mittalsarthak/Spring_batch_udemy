package com.learn.sbb.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HwStepExecutionListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Before the step execution: " + stepExecution.getJobExecution().getExecutionContext());
//        stepExecution.getJobExecution().getExecutionContext().put("Some other name","1211");
        System.out.println("Inside the step - job parameter" + stepExecution.getJobExecution().getJobParameters());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("After the step execution: " + stepExecution.getJobExecution().getExecutionContext());
        return null;
    }
}

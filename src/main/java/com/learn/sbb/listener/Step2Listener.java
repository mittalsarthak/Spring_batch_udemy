package com.learn.sbb.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class Step2Listener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("******* Before Step-2 ********");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("******** After Step-2 ********");
        return null;
    }
}

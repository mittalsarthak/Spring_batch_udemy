package com.learn.sbb.config;

import com.learn.sbb.SbbApplication;
import com.learn.sbb.listener.HwJobExecutionListener;
import com.learn.sbb.listener.HwStepExecutionListener;
import com.learn.sbb.listener.Step2Listener;
import com.learn.sbb.processor.InMemoryItemProcessor;
import com.learn.sbb.reader.InMemoryReader;
import com.learn.sbb.writer.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Console;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {
    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private HwJobExecutionListener hwJobExecutionListener;

    @Autowired
    private HwStepExecutionListener hwStepExecutionListener;

    @Autowired
    private InMemoryReader inMemoryReader;

    @Autowired
    private InMemoryItemProcessor inMemoryItemProcessor;

    @Autowired
    private ConsoleItemWriter consoleItemWriter;

    @Autowired
    private Step2Listener step2Listener;

    private Tasklet helloWorldTasklet() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello World...");
                return RepeatStatus.FINISHED;
            }
        };
    }


    @Bean
    public Step step1() {
        return steps.get("step1")
                .listener(hwStepExecutionListener)
                .tasklet(helloWorldTasklet())
                .build();
    }

    public Step step2() {
        return steps.get("step2")
                .listener(step2Listener).
                <Integer, Integer>chunk(3)
                .reader(inMemoryReader)
                .processor(inMemoryItemProcessor)
                .writer(consoleItemWriter)
                .build();
    }

    @Bean
    public Job helloWorldJob() {
        return jobs.get("helloWorldJob")
                .incrementer(new RunIdIncrementer())
                .listener(hwJobExecutionListener)
                .start(step1())
                .next(step2())
                .build();
    }
}

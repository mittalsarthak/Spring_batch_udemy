package com.learn.sbb.config;

import com.learn.sbb.listener.HwJobExecutionListener;
import com.learn.sbb.listener.HwStepExecutionListener;
import com.learn.sbb.listener.Step2Listener;
import com.learn.sbb.model.Product;
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
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;

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
    public FlatFileItemReader flatFileItemReader() {
        FlatFileItemReader reader = new FlatFileItemReader();

        //Step 1: Let the reader know where is the file
        reader.setResource(new FileSystemResource("input/products.csv"));

        //Step 2: create the line mapper
        reader.setLineMapper(
                new DefaultLineMapper(){
                    {
                        setLineTokenizer(new DelimitedLineTokenizer(){
                            {
                                setNames(new String[] {"productId","productName","productDesc","price","unit"});
                            }
                        });
                        setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>(){
                            {
                                setTargetType(Product.class);
                            }
                        });
                    }
                }
        );
        //Step 3: Tell reader to skip the header
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public Step step1() {
        return steps.get("step1")
                .listener(hwStepExecutionListener)
                .tasklet(helloWorldTasklet())
                .build();
    }

    @Bean
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
    public Step step3() {
        return steps.get("step3")
                .<String, String>chunk(1)
                .reader(flatFileItemReader())
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
                .next(step3())
                .build();
    }
}

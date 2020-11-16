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
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.sql.DataSource;
import java.io.File;
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

    @Autowired
    private DataSource dataSource;

    private Tasklet helloWorldTasklet() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello World...");
                return RepeatStatus.FINISHED;
            }
        };
    }

    // Reader for csv file
    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(
            @Value("#{jobParameters['inputFile']}")
            FileSystemResource inputFile) {
        FlatFileItemReader reader = new FlatFileItemReader();

        //Step 1: Let the reader know where is the file
        //Hard coding the input file. If using this then we don't need @value in method declaration
        //reader.setResource(new FileSystemResource("input/products.csv"));

        //Late Binding of parameter
        reader.setResource(inputFile);

        //Step 2: create the line mapper
        reader.setLineMapper(
                new DefaultLineMapper(){
                    {
                        setLineTokenizer(new DelimitedLineTokenizer(){
                            {
                                setNames(new String[] {"productId","productName","productDesc","price","unit"});
//                                to set the delimiter
                                setDelimiter("|");
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

    @StepScope
    @Bean
    //Reader for fixed length file
    public FlatFileItemReader fixFlatFileItemReader(
            @Value("#{jobParameters['inputFile']}")
            FileSystemResource inputFile
    ){

        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(inputFile);

        reader.setLineMapper(
                new DefaultLineMapper(){
                    {
                        setLineTokenizer(new FixedLengthTokenizer(){
                            {
                                setNames("productId","productName","productDesc","price","unit");
                                setColumns(
                                        new Range(1,16),
                                        new Range(17,41),
                                        new Range(42,65),
                                        new Range(66,73),
                                        new Range(74,80)
                                );
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

        reader.setLinesToSkip(1);
        return reader;
    }

    //Reader for xml file
    @StepScope
    @Bean
    public StaxEventItemReader xmlitemReader(
            @Value("#{jobParameters['inputFile']}")
            FileSystemResource inputFile){
        StaxEventItemReader reader = new StaxEventItemReader();
        //where to read the xml file
        reader.setResource(inputFile);

        //need to let reader know which tags describe the domain object
        reader.setFragmentRootElementName("product");

        //tell the reader how to parse XML and which domain object to be mapped
        reader.setUnmarshaller(new Jaxb2Marshaller(){
            {
                setClassesToBeBound(Product.class);
            }
        });
        return reader;
    }

    @Bean
    public JdbcCursorItemReader jdbcCursorItemReader(){
        JdbcCursorItemReader reader = new JdbcCursorItemReader();
        reader.setDataSource(this.dataSource);
        reader.setSql("SELECT * FROM products;");
        reader.setRowMapper(new BeanPropertyRowMapper(){
            {
                setMappedClass(Product.class);
            }
        });
        return reader;
    }

    @StepScope
    @Bean
    public JsonItemReader jsonItemReader(
            @Value("#{JobParameters['inputFile']}")
            FileSystemResource inputFile
    ){
        JsonItemReader reader = new JsonItemReader(inputFile,new JacksonJsonObjectReader(Product.class));
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
//                .reader(flatFileItemReader(null))
//                .reader(xmlitemReader(null))
//                .reader(fixFlatFileItemReader(null))
//                .reader(jdbcCursorItemReader())
                .reader(jsonItemReader(null))
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

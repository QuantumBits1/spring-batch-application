package com.spring.batch.job;

import com.spring.batch.dto.EmployeeDTO;
import com.spring.batch.mapper.EmployeeDBRowMapper;
import com.spring.batch.mapper.EmployeeFileRowMapper;
import com.spring.batch.model.Employee;
import com.spring.batch.processor.EmployeeProcessor;
import com.spring.batch.writer.EmailSenderWriter;
import com.spring.batch.writer.EmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;

@Configuration
public class Demo1 {

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeProcessor employeeProcessor;
    //private EmployeeDBWriter employeeDBWriter;
    private DataSource dataSource;
    //private Resource outputFileResource = new FileSystemResource("output/employee_output.csv");

    @Autowired
    public Demo1(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                 EmployeeProcessor employeeProcessor, EmployeeDBWriter employeeDBWriter, DataSource dataSource){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        //this.employeeDBWriter = employeeDBWriter;
        this.dataSource = dataSource;
    }

    @Qualifier(value = "demo1")
    @Bean
    public Job demo1Job() throws Exception {
        return this.jobBuilderFactory.get("demo1")
                .start(step1Demo1())
                //.next(step2Demo1())  //use this for multistep
                .build();
    }

    @Bean
    public Step step1Demo1() throws Exception {
        return this.stepBuilderFactory.get("step1")
                .<EmployeeDTO, Employee>chunk(5)
                .reader(employeeFileReader())
                .writer(employeeDBWriterDefault())
                .processor(employeeProcessor)  //optional
                .taskExecutor(taskExecutor())
                .build();
    }

    //Use this when need multistep
    /*@Bean
    public Step step2Demo1() throws Exception {
        return this.stepBuilderFactory.get("step2")
                .<Employee, EmployeeDTO>chunk(10)
                .reader(employeeDBReader())
//                .writer(employeeFileWriter())
                .writer(emailSenderWriter())
                .build();
    }*/

    @Bean
    @StepScope
    Resource inputFileResource(@Value("#{jobParameters[fileName]}") final String fileName) throws Exception {
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeFileReader() throws Exception {
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<>();
        reader.setResource(inputFileResource(null));
        reader.setLineMapper(new DefaultLineMapper<EmployeeDTO>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("employeeId", "firstName", "lastName", "email", "age");
                setDelimiter(",");
            }});
            setFieldSetMapper(new EmployeeFileRowMapper());
        }});
        return reader;
    }

    @Bean
    public JdbcBatchItemWriter<Employee> employeeDBWriterDefault() {
        JdbcBatchItemWriter<Employee> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("insert into employee (employee_id, first_name, last_name, email, age) " +
                "values (:employeeId, :firstName, :lastName, :email, :age)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return itemWriter;
    }

    //Use this for multiste
    /*@Bean
    public ItemStreamReader<Employee> employeeDBReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDBRowMapper());
        return reader;
    }*/

    //Use this for multistep
    /*@Bean
    public ItemWriter<EmployeeDTO> employeeFileWriter() throws Exception {
        FlatFileItemWriter<EmployeeDTO> writer = new FlatFileItemWriter<>();
        writer.setResource(outputFileResource);
        writer.setLineAggregator(new DelimitedLineAggregator<EmployeeDTO>() {
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<EmployeeDTO>() {
                    {
                        setNames(new String[]{"employeeId", "firstName", "lastName", "email", "age"});
                    }
                });
            }
        });
        writer.setShouldDeleteIfExists(true);
        return writer;
    }*/

    @Bean
    EmailSenderWriter emailSenderWriter() {
        return new EmailSenderWriter();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }
}

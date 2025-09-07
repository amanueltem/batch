package com.amman.batch.config;

import com.amman.batch.student.*;

import lombok.RequiredArgsConstructor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {
	private final StudentRepository repository;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager platformTransactionManager;

	@Bean
	public FlatFileItemReader<Student> itemReader() {
		FlatFileItemReader<Student> itemreader = new FlatFileItemReader<Student>();
		itemreader.setResource(new FileSystemResource("src/main/resources/students.csv"));
		itemreader.setName("csvReader");
		itemreader.setLinesToSkip(1);
		itemreader.setLineMapper(lineMapper());
		return itemreader;
	}

	@Bean
	public StudentProcessor processor() {
		return new StudentProcessor();
	}

	@Bean
	public RepositoryItemWriter<Student> writer() {
		RepositoryItemWriter<Student> writer = new RepositoryItemWriter<Student>();
		writer.setRepository(repository);
		writer.setMethodName("save");
		return writer;
	}
	@Bean
	public Step importStep() {
		return new StepBuilder("csvImport",jobRepository)
				   .<Student,Student>chunk(10,platformTransactionManager)
				   .reader(itemReader())
				   .processor(processor())
				   .writer(writer())
				   .build();
	}
	@Bean
	public Job runJob() {
		return new JobBuilder("importStudents",jobRepository)
				     .start(importStep())
				     .build();
	}

	private LineMapper<Student> lineMapper() {
		DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<Student>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id", "firstname", "lastname", "age");

		BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<Student>();

		fieldSetMapper.setTargetType(Student.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}
}

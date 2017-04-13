package com.exbatch.config;

import com.exbatch.batch.*;
import com.exbatch.domain.User;
import com.exbatch.repository.StringDB;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.convert.Property;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.List;
import java.util.Properties;

@Configuration
@Import({DBConfig.class})
@ComponentScan({"com.exbatch"})
@PropertySource(value = {
		"classpath:/config.properties"
})
@ImportResource("classpath:/transaction.xml")
@EnableBatchProcessing
@EnableTransactionManagement
public class AppConfig {
	@Autowired
	ApplicationContext context;

	@Autowired
	Environment environment;

	@Value("${mybatis.url}")
	String mybatisUrl;

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	StringDB alphabetRepository;

	@Bean
	public static PropertySourcesPlaceholderConfigurer propertyConfigIn() {
		return new PropertySourcesPlaceholderConfigurer();
	}

	@Bean
	public ResourcelessTransactionManager resourcelessTransactionManager() {
		ResourcelessTransactionManager resourcelessTransactionManager = new ResourcelessTransactionManager();
		return resourcelessTransactionManager;
	}

	@Bean
	public DataSourceTransactionManager dataSourceTransactionManager() {
		DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
		dataSourceTransactionManager.setDataSource(dataSource());
		return dataSourceTransactionManager;
	}

	@Bean
	public SimpleJobLauncher jobLauncher() throws  Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository());
		return jobLauncher;
	}

	@Bean(destroyMethod = "close")
	public BasicDataSource dataSource() {
		BasicDataSource dataSource = new BasicDataSource();

		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/db_tplus?useUnicode=yes&amp;characterEncoding=UTF-8");
		dataSource.setUsername("root");
		dataSource.setPassword("");

		return dataSource;
	}

	@Bean
	public  SqlSessionFactory sqlSessionFactory() throws Exception {

		PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
		Resource[] resources = pathMatchingResourcePatternResolver.getResources("classpath*:database/**");

		SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
		sqlSessionFactoryBean.setDataSource(dataSource());
		sqlSessionFactoryBean.setMapperLocations(resources);
		return sqlSessionFactoryBean.getObject();
	}

	@Bean(destroyMethod = "clearCache")
	public SqlSessionTemplate sqlSessionTemplate() throws Exception {
		SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory());
		return sqlSessionTemplate;
	}

	@Bean
	public JobRepository jobRepository() throws Exception {
		JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
		jobRepositoryFactoryBean.setDataSource(dataSource());
		jobRepositoryFactoryBean.setTransactionManager(dataSourceTransactionManager());
		jobRepositoryFactoryBean.setDatabaseType("mysql");

		return (JobRepository) jobRepositoryFactoryBean.getObject();
	}

	@Bean
	public JobRepository mapJobRepository() throws Exception {
		MapJobRepositoryFactoryBean factoryBean = new MapJobRepositoryFactoryBean();
		factoryBean.setTransactionManager(resourcelessTransactionManager());
		return (JobRepository) factoryBean.getObject();
	}


	@Bean
	CustomItemReader customItemReader() {
		return new CustomItemReader();
	}

	@Bean
	CustomItemWriter customItemWriter() {
		return new CustomItemWriter();
	}

	@Bean
	CustomItemProcessor customItemProcessor() {
		return new CustomItemProcessor();
	}

	@Bean(name = "batchProperties", autowire = Autowire.BY_NAME)
	Properties batchProperties() {
		Properties properties = new Properties();
		properties.put("batch_name", "mybatch");
		return properties;
	}

	@Bean
	protected Step step1() throws Exception {
		return this.steps.get("#ExChunkBatch-step1").<String, String>chunk(3)
				.reader(customItemReader())             ///
				.processor(customItemProcessor())   ///
				.writer(customItemWriter())         ///
				.build();
	}


	@Bean
	CustomDbItemReader customDbItemReader() {
		return new CustomDbItemReader();
	}

	@Bean
	CustomDbItemProcessor customDbItemProcessor() {
		return new CustomDbItemProcessor();
	}

	@Bean
	CustomDbItemWriter customDbItemWriter() {
		return new CustomDbItemWriter();
	}

	@Bean
	protected Step dbStep() throws Exception {

		return this.steps.get("#ExDbBatch-dbStep").<List<User>, List<User>>chunk(1)
				.reader(customDbItemReader())             ///
				.processor(customDbItemProcessor())   ///
				.writer(customDbItemWriter())         ///
				.build();
	}

	@Bean(name="sampleJob", autowire = Autowire.BY_NAME)
	public Job sampleJob() throws Exception {
		return this.jobs.get("#ExChunkBatch").start(step1()).build();
	}

	@Bean(name = "sampleDbJob", autowire = Autowire.BY_NAME)
	public Job sampleDbJob() throws Exception {
		return this.jobs.get("#ExDbBatch").start(dbStep()).build();
	}
}

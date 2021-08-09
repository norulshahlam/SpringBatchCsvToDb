package shah.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import shah.model.Product;
@Configuration
public class BatchConfig {

	@Autowired
	private JobBuilderFactory job;
	
	@Autowired
	private StepBuilderFactory step;
	
	@Bean
	public Job job() {
		return job
		.get("job")
		.incrementer(new RunIdIncrementer())
		.start(step())
		.build();
	}
	@Bean
	public Step step() {
		return step
				.get("step")
				.<Product,Product>chunk(5)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}
	
	@Bean
	public ItemReader<Product> reader() {
		
		System.out.println("in reader");
		// obj for locating file
		FlatFileItemReader<Product> reader = new FlatFileItemReader<>();

		// set location of file to read
		reader.setResource(new ClassPathResource("products.csv"));

		// obj to read values
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

		// read each row and set into its own fields
		lineTokenizer.setNames("id", "name", "description", "price");

		// take the parse values and set it into the beans, obj to target class type
		BeanWrapperFieldSetMapper<Product> fieldSetMapper = new BeanWrapperFieldSetMapper<>();

		// target the class type
		fieldSetMapper.setTargetType(Product.class);

		// obj to map each line into product obj
		DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();

		// take the value read & class type and add here
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		reader.setLineMapper(lineMapper);
		return reader;

	}
	//	input type, output type
	@Bean
	public ItemProcessor<Product, Product> processor(){


		System.out.println("in processor");
		return (p) ->{
			p.setPrice(p.getPrice()- 5);
			System.out.println("Processing: "+p.getId()+", "+p.getName()+", "+p.getDescription()+", "+p.getPrice());
			return p;
		};
		
		
	}
	@Bean
	public ItemWriter<Product> writer(){

		System.out.println("in writer");
		JdbcBatchItemWriter<Product> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(datasource());
		writer.setItemSqlParameterSourceProvider(new 			BeanPropertyItemSqlParameterSourceProvider<Product>()); 
		
		writer.setSql("INSERT INTO PRODUCT (ID,NAME,DESCRIPTION,PRICE) VALUES 			(:id,:name,:description,:price)");
		return writer;
	}
	
	@Bean
	public DataSource datasource() {

		DriverManagerDataSource datasource = new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl("jdbc:mysql://localhost:3306/login_app");
		datasource.setUsername("root");
		datasource.setPassword("root");
		
		return datasource;
	}

}

















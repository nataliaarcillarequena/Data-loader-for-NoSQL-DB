package com.nat.main;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.datastax.oss.driver.shaded.guava.common.io.Files;
import com.nat.main.author.Author;
import com.nat.main.author.AuthorRepository;
import com.nat.main.book.Book;
import com.nat.main.book.BookRepository;
import com.nat.main.connection.DataStaxAstraProperties;

@SpringBootApplication
//need to enable this class in the root class (this main one) as it enables the config properties 
@EnableConfigurationProperties(DataStaxAstraProperties.class)
//@EnableCassandraRepositories(basePackages = {"com.nat.author", "com.nat.book"})
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepo;
	
	@Autowired
	BookRepository bookRepo;
	
	//accessing the .txt files (author and works dumps)
	@Value("${datadump.location.author}")
	private String authorDumpLocation;
	
	@Value("${datadump.location.works}")
	private String worksDumpLocation;
	
	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}
	
	//putting authors into author_by_id table in datatstax cassandra 
	private void initAuthors() {
		
		//get the author dump location into a path variable 
		Path path = Paths.get(authorDumpLocation);
		
		//read the .txt file line by line and sends to astra to save each lines
		try (Stream<String> lines = java.nio.file.Files.lines(path)){
			
			lines.forEach(line -> {
				//read and parse the line - constructing json object from the lines (by finding position of first {- dont need all the stuff before in each line)
				String jsonString = line.substring(line.indexOf("{")); ; //gets the position of the 1st curly brace and forms a substring starting from it
				
				//construct the author object
				try {
					JSONObject jsonObject = new JSONObject(jsonString); //!!!!!!!!!CHECK if its the correct import 
					Author author = new Author();
					author.setName(jsonObject.optString("name"));  //opt string returns null if doesn't find "name" as a field in the json object 
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", "")); //gets the id from the json object- replaces /authors/ with empty string to just get the id
					
					//persist using repository
					System.out.println("Saving author " + author.getName());
					authorRepo.save(author);	
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	//putting the works into cassandra table 
	private void initWorks() {
		
		Path path = Paths.get(worksDumpLocation);
		
		//read the .txt file line by line
		try (Stream<String> lines= java.nio.file.Files.lines(path)){
			
			//each line gets json object parsed 
			lines.limit(50).forEach(line -> {
				
				//get the json object starting from relevant position 
				String jsonString = line.substring(line.indexOf("{"));
				
				//construct the book object from fields of the json object 
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Book book = new Book();
					
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					
					
					book.setName(jsonObject.optString("title"));
					//for the decription, we have to get the "value" string inside the "descriptio" JSON object 
					JSONObject descriptionObj = jsonObject.optJSONObject("description");
					if(descriptionObj !=null) {
						book.setDescription(descriptionObj.optString("value"));
					}
					
					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if(publishedObj != null) {
						String dateStr = publishedObj.getString("value");
						//can also create own date time formatter 
						book.setPublishedDate(LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(dateStr)));
					}
					
					JSONArray coversJsonArray = jsonObject.optJSONArray("covers");
					if(coversJsonArray !=null) {
						List<String> coverIds = new ArrayList<>();
						for(int i=0; i<coversJsonArray.length(); i++) {
							coverIds.add(coversJsonArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}
					
					JSONArray authorsJsonArray = jsonObject.optJSONArray("authors");
					if(authorsJsonArray != null) {
						List<String> authorIds = new ArrayList<>();
						for(int i=0; i<authorsJsonArray.length(); i++) {
							JSONObject obj = authorsJsonArray.getJSONObject(i);
							JSONObject auth = obj.optJSONObject("author");
							String authId = auth.optString("key").replace("/authors/", "");
							
							authorIds.add(authId);
						}
						book.setAuthorId(authorIds);
						
						//make call to author repo on cassandra to fetch the author names- give it the author ids and get the corresponding names 
						List<String> authorNames = authorIds.stream().map(id -> authorRepo.findById(id)) //finds author id (from works) in author table
							.map(optionalAuth -> {
								if(!optionalAuth.isPresent()) return "Unknown Author";// when author id exists in works but not able to find author in author table
								return optionalAuth.get().getName(); //from the authorId, gets the author name
							}).collect(Collectors.toList()); //collects all the author id's and corresponding names into a list 
					
						book.setAuthorNames(authorNames);	
					}
					
					//persist using repository
					System.out.println("Saving book " + book.getName());
					bookRepo.save(book);
					
				} catch (JSONException e) { //!!!!!! can change to any exception 
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//post construct = after everything has been constructed, run this method
	@PostConstruct
	public void start() {
		
		//initAuthors();
		initWorks();
		
	}
	
	
	//bean which allows connection to datastax 
	//tells where the secure bundle file is (the path to it) so can use that batabase thats active on datastax 
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}

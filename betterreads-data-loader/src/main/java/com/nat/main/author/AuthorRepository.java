package com.nat.main.author;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

//used for fetching from and persisting to cassandra 
@Repository
public interface AuthorRepository extends CassandraRepository<Author, String> {

	
	
	
	
	
}

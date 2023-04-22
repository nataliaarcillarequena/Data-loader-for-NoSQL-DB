package com.nat.main.book;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

//used for fetching from and persisting to cassandra 
@Repository
public interface BookRepository extends CassandraRepository<Book, String> {

	
	
	
	
	
	
}

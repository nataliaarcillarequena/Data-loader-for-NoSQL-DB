package com.nat.main.author;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.CassandraType.Name;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

//entity class - need to give it annotations like in JAP (but here cassandra database)
//table called author by id 
@Table(value = "author_by_id")
public class Author {

	//primary key column with name "author_id" in the table, making it a partition key 
	//ordinal = order of this column relative to the other primary key columns
	@Id @PrimaryKeyColumn(name = "author_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String id;
	
	@Column("author_name")
	@CassandraType(type = Name.TEXT) //dont need to specify the size of the text for NoSQL
	private String name;
	
	@Column("personal_name")
	@CassandraType(type = Name.TEXT)
	private String personalName;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPersonalName() {
		return personalName;
	}

	public void setPersonalName(String personalName) {
		this.personalName = personalName;
	}
	
	
	
}

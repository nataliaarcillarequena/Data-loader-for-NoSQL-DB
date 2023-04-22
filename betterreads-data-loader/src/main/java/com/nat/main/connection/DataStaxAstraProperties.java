package com.nat.main.connection;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxAstraProperties {
	
	//we are exposing the datastax.astra in the application.yml file as a property 
	private File secureConnectBundle;

	public File getSecureConnectBundle() {
		return secureConnectBundle;
	}

	public void setSecureConnectBundle(File secureConnectBundle) {
		this.secureConnectBundle = secureConnectBundle;
	}

	
	
}

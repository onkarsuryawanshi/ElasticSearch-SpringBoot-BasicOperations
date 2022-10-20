package com.es;

import com.es.service.StartConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Application {
	public static void main(String[] args) throws IOException {
		SpringApplication.run(Application.class, args);
		System.out.println("running");
		StartConsumer startConsumer = new StartConsumer();
		startConsumer.execute();

		System.out.println("shutting down");
	}


}

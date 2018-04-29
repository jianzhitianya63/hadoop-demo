package com.hadoop.outlog;

import com.hadoop.outlog.log.GenerateLog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OutlogApplication {

	public static void main(String[] args) {
		SpringApplication.run(OutlogApplication.class, args);
		try {
			GenerateLog.generLog();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

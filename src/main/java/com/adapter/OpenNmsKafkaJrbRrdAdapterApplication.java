package com.adapter;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.adapter.services.AdapterService;

@EnableScheduling
@SpringBootApplication
public class OpenNmsKafkaJrbRrdAdapterApplication {
	@Autowired
	AdapterService adapterService;
	@Value("${thread.pool.size}")
	private int poolSize;
	
	public static void main(String[] args) {
		SpringApplication.run(OpenNmsKafkaJrbRrdAdapterApplication.class, args);
	}
	
	@Scheduled(cron="0 0,5,10,15,20,25,30,35,40,45,50,55 * * * *")
	public void run() throws Exception {
		long now = System.currentTimeMillis();
		long pollDate=TimeUnit.MILLISECONDS.toSeconds(now-5*60*1000);
		adapterService.FetchRRDData(pollDate,pollDate,poolSize);
		System.out.println("RRD data Poll at  "+new Timestamp(pollDate*1000));
		System.out.println("Job start at : "+new Timestamp(now));
		System.out.println("Job end at : "+new Timestamp(System.currentTimeMillis()));
		
	}

}

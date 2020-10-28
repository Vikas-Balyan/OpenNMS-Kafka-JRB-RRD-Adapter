package com.adapter.services.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.jrobin.core.RrdException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.adapter.constants.InMemoryData;
import com.adapter.services.AdapterService;
import com.adapter.services.thread.RRDConcurrencyControl;


@Service
public class AdapterServiceImpl implements AdapterService {
	@Value("${rrd.files.root.path}")
	private String rootPath; 
	List<String> topics=null;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${nms.ip}")
    private String nmsIp;

	

	@Override
	public int FetchRRDData(long startDate, long endDate,int poolSize) throws IOException, SQLException, RrdException, InterruptedException, ExecutionException {
		System.out.println("Kafka Topics loading...");
		topics=topicList();
		System.out.println("NMS RRD or  JRB Files Path Scrapping Start...");
		System.out.println("NMS RRD or  JRB Root Path : "+rootPath);
		List<String> filesPath=scrapAllFiles(rootPath);
		System.out.println("NMS RRD or  JRB Files Path Scrapping Complted...No Of File Paths : "+filesPath.size());
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		List< Future<String> > futureList = new ArrayList<Future<String>>();
		for(int index=0;index<filesPath.size();index++) {
			String path=filesPath.get(index);
			futureList.add(executor.submit(new RRDConcurrencyControl(nmsIp,path, startDate, endDate)));
		}
		 for (Future<String> f :futureList)
		 {
			 if(f.get()!=null) {
			 JSONObject msg=new JSONObject(f.get());
			        String topic=msg.getString("topic");
			        if(topics.contains(topic)) {
			        	kafkaTemplate.send(topic,msg.toString());	
			        }else {
			        	String t=createTopic(topic);
			        	topics.add(t);
			        	kafkaTemplate.send(t,msg.toString());
			        }
			 }
			        
		  }
		 kafkaTemplate.flush();
		executor.shutdown();
		return 0;
	}
	


	private static List<String> scrapAllFiles(String rootPath) throws IOException {
		Path start = Paths.get(rootPath);
		Stream<Path> stream = Files.walk(start, Integer.MAX_VALUE);
		    List<String> collect = stream
		        .map(String::valueOf)
		        .sorted()
		        .filter(f -> f.endsWith(".jrb"))
		        .collect(Collectors.toList());
		    return collect;
	}

	

	public String createTopic(String topic) {
		AdminClient adminClient = AdminClient.create(InMemoryData.propertiesConfigs());
		NewTopic newTopic = new NewTopic(topic, 1, (short)1);
		List<NewTopic> newTopics = new ArrayList<NewTopic>();
		newTopics.add(newTopic);
		adminClient.createTopics(newTopics);
		adminClient.close();
		return topic;
	}
	public List<String> topicList() {
		Map<String, List<PartitionInfo> > topics;
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(InMemoryData.propertiesConfigs());
		topics = consumer.listTopics();
		consumer.close();
		return topics.keySet().stream().collect(Collectors.toList()); 
	}


}



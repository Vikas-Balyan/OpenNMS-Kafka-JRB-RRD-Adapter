package com.adapter.constants;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.springframework.context.annotation.Bean;

public class InMemoryData {
    public static Properties propertiesConfigs() {
    	Properties props = new Properties();
		props.put("bootstrap.servers", "172.27.74.9:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

	
}

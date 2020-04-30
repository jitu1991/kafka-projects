package com.udemy.kafka.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	public static void main(String[] args) {
		String bootstrapServers = "localhost:9092";
		
		Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
		//Create Producer Properties
		Properties properties = new Properties();
		/*properties.setProperty("bootstrap.server", bootstrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());*/
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Create ProducerRecord
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

		//Send Data
		//producer.send(record);
		
		producer.send(record);
		
		producer.flush();
		producer.close();
	}
}

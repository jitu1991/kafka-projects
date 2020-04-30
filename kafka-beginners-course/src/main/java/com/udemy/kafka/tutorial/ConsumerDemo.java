package com.udemy.kafka.tutorial;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		String bootstrapServers = "localhost:9092";
		String groupId = "my-fifth-apps";
		String topic = "topic1";
		
		//Create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//values - earliest/latest/none
		
		//Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		//subscribe to topic
		consumer.subscribe(Collections.singleton(topic));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));//new in kafka 2.0.0
			
			for(ConsumerRecord<String, String> record : consumerRecords) {
				logger.info("key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
		}
		
	}
}

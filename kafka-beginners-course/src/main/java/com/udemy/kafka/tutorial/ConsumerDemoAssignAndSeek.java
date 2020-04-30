package com.udemy.kafka.tutorial;

import java.time.Duration;
import java.util.Arrays;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
		
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

		//assign and seek are mostly used to replay data or fetch a specific message
		//assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		int noOfMessageToRead = 5;
		boolean keepOnReading = true;
		int noOfMessageReadSoFar = 0;
		
		while(keepOnReading) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));//new in kafka 2.0.0
			
			for(ConsumerRecord<String, String> record : consumerRecords) {
				logger.info("key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				if(noOfMessageReadSoFar >= noOfMessageToRead) {
					keepOnReading = false;
					break;
				}
			}
		}
		logger.info("exiting application");
	}
}

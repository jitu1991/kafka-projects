package com.udemy.kafka.tutorial;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String bootstrapServers = "localhost:9092";

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		// Create Producer Properties
		Properties properties = new Properties();
		/*
		 * properties.setProperty("bootstrap.server", bootstrapServers);
		 * properties.setProperty("key.serializer", StringSerializer.class.getName());
		 * properties.setProperty("value.serializer", StringSerializer.class.getName());
		 */

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {

			String topic = "new_topic";
			String message = "hello world " + i;
			String key = "id_"+i;
			// Create ProducerRecord
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,
					message);

			// Send Data
			// producer.send(record);
			logger.info("Key: " + key);
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a message is sent or exception thrown
					if (e == null) {
						logger.info(
								"Received new metadata.\n" + 
						"Topic: " + recordMetadata.topic() + "\n" + 
						"Partition: " + recordMetadata.partition() + "\n" + 
						"Offset: " + recordMetadata.offset() + "\n" + 
						"Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			}).get();
		}
		producer.flush();
		producer.close();
	}
}

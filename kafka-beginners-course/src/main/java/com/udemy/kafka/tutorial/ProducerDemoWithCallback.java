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

public class ProducerDemoWithCallback {
	public static void main(String[] args) {
		String bootstrapServers = "localhost:9092";

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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

			// Create ProducerRecord
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic1",
					"hello world " + i);

			// Send Data
			// producer.send(record);

			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a message is sent or exception thrown
					if (e == null) {
						logger.info(
								"Received new metadata.\n" + "Topic: " + recordMetadata.topic() + "\n" + "Partition: "
										+ recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}
		producer.flush();
		producer.close();
	}
}

package com.udemy.kafka.tutorial;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private ConsumerDemoWithThread() {
	}

	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

		String bootstrapServers = "localhost:9092";
		String groupId = "my-sixth-app";
		String topic = "new_topic";

		CountDownLatch latch = new CountDownLatch(1);

		logger.info("Creating consumer thread");
		Runnable myConsumerRunnable = new ConsumerThread(latch, bootstrapServers, groupId, topic);

		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("caught shutdown hook");
			((ConsumerThread) myConsumerRunnable).shutdown();
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("application interrupted with error " + e);
		} finally {
			logger.info("application is closing");
		}
	}

	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

		public ConsumerThread(CountDownLatch latch, String bootstrapServer, String groupId, String topic) {
			this.latch = latch;

			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// values - earliest/latest/none

			consumer = new KafkaConsumer<String, String>(properties);
		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));// new in
																											// kafka
																											// 2.0.0

					for (ConsumerRecord<String, String> record : consumerRecords) {
						logger.info("key: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}

		public void shutdown() {
			consumer.wakeup(); // Method to interrupt consumer.poll()
		}
	}
}

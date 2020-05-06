package com.udemy.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.udemy.kafka.tutorial.ConsumerDemoWithThread.ConsumerThread;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	String consumerKey = "RCzZwMT4aIZCpuwgKGlyysfsQ";
	String consumerSecret = "G7ZRKKqtjCfSds32fhsOTgkvDDJ8U1E1PXqahcNcudPgIqcFS6";
	String token = "2757967215-lsqXRaCWiYwQYFOes2ZZS5qi2PjB3H5EZc4GuPR";
	String secret = "hU9X7sawkp1AXtz8ZNPG9BdfCRcklC5gZL5k90jzozOoz";

	public TwitterProducer() {
	}

	public static void main(String[] args) {

		new TwitterProducer().run();
	}

	public void run() {
		logger.info("Setting up...");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect();

		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// loop to send tweets to kafka

		// Add shutdown hook
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					logger.info("caught shutdown hook");
					client.stop();
					producer.close();
				}));
				
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if(e != null) {
							logger.error("Something bad happened", e);
						}
					}
				});
			}
		}
		logger.info("End of application");

	}

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServers = "localhost:9092";
		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Creating client
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		hosebirdClient.connect();
		return hosebirdClient;
	}

}

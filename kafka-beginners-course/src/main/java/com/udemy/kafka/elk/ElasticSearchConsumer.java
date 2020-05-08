package com.udemy.kafka.elk;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String bootstrapServers = "localhost:9092";
		String groupId = "kafka-demo-elasticsearch";

		// Create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// values - earliest/latest/none
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//disable auto commit offset
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
		

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));//new in kafka 2.0.0
			
			logger.info("Received " + consumerRecords.count() + " records");
			for(ConsumerRecord<String, String> record : consumerRecords) {
				//2 Strategies for key - Idempotence
				//kafka generic ID
				//String id = record.topic() + record.partition() + record.offset();
				
				//Feed specific id
				//String id = extractId(record.value());
				
				String jsonString = record.value();
				
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexResponse.getId();
				logger.info(id);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				logger.info("committing offset");
				consumer.commitSync();
				logger.info("offset committed");
				client.close();
			}
		}
	}

	private static JsonParser jsonParser = new JsonParser();
	private static String extractId(String json) {
		return jsonParser.parse(json).getAsJsonObject().get("id_str").getAsString();
	}

	public static RestHighLevelClient createClient() {
		String hostName = "kafka-course-8483528101.eu-west-1.bonsaisearch.net";
		String userName = "tthsvxhtsi";
		String password = "2vw3545uty";

		// Not required in case of local elastic search
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
}

package com.github.jlmolinyawe.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
	private static JsonParser jsonParser = new JsonParser();

	public static RestHighLevelClient createClient() {
		String hostname = "kafka-course-4047378788.us-west-2.bonsaisearch.net";
		String username = "fzgn4mamab";
		String password = "we43mvtdnb";

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder restClientBuilder = RestClient
				.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

		return new RestHighLevelClient(restClientBuilder);
	}

	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();

		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			Integer recordCount = consumerRecords.count();
			logger.info("Received " +  recordCount + " records!");

			BulkRequest bulkRequest = new BulkRequest();
			for (ConsumerRecord<String,String> consumerRecord: consumerRecords) {
				// Generic kafkaID
				// String id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset();
				String id;
				// Twitter feed specific unique ID
				try {
					id = extractIdFromTweet(consumerRecord.value());
				} catch (NullPointerException e) {
					id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset();
				}
				String jsonString = consumerRecord.value();
				logger.info(id);
				logger.info(jsonString);
				// insert into elastic
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(jsonString, XContentType.JSON);
				bulkRequest.add(indexRequest);
			}

			if (recordCount > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committing offsets...");
				consumer.commitSync();
				logger.info("Offsets has been committed");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

		// Close client gracefully
//		client.close();
	}

	private static String extractIdFromTweet(String tweetJson) {
		return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public static KafkaConsumer<String,String> createConsumer(String topic) {
		String bootStrapServers = "localhost:9092";
		String groupId = "kafka-demo-elasticsearch";

		// Set properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable autocommit
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

		// Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Collections.singleton(topic));

		return consumer;
	}
}

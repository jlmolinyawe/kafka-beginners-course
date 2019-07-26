package com.github.jlmolinyawe.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	private static final String bootStrapServers = "localhost:9092";
	private static final String groupId = "my-forth-app";
	private static final String topic = "first_topic";
	public static void main(String[] args) {
		// Set properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// Subscribe to topic
		consumer.subscribe(Collections.singleton(topic));

		// Wait for data
		while (true) {
//			consumer.poll(100);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord consumerRecord: consumerRecords) {
				logger.info("Key: " + consumerRecord.key() + ". Value: " + consumerRecord.value());
				logger.info("Partition: " + consumerRecord.partition());
				logger.info("Offset: " + consumerRecord.offset());
			}
		}
	}
}

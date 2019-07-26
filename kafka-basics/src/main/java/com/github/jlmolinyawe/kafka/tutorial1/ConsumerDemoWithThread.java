package com.github.jlmolinyawe.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
	private static final String bootStrapServers = "localhost:9092";
	private static final String groupId = "my-sixth-app";
	private static final String topic = "first_topic";
	public static void main(String[] args) {
		CountDownLatch  latch = new CountDownLatch(1);

		Runnable myConsumerThrear = new ConsumerThread(latch);


	}

	public static class ConsumerThread implements Runnable {
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

		public ConsumerThread(CountDownLatch latch) {
			this.latch = latch;

			// Set properties
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			this.consumer = new KafkaConsumer<>(properties);
		}

		@Override
		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord consumerRecord: consumerRecords) {
						logger.info("Key: " + consumerRecord.key() + ". Value: " + consumerRecord.value());
						logger.info("Partition: " + consumerRecord.partition());
						logger.info("Offset: " + consumerRecord.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();

				// Tell main code is done w/ consumer
				latch.countDown();
			}

		}

		public void shutDown() {
			// Interrupts consumer.poll().  Throws WakeUpException
			consumer.wakeup();
		}

	}

}

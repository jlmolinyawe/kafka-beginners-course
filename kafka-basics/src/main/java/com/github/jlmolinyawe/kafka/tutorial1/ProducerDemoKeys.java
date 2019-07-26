package com.github.jlmolinyawe.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

	private static final String bootstrapServers = "localhost:9092";
	private static final String topicName = "first_topic";
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		System.out.println("HELLO");

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {
			String key = "id_" + i;
			// create data
			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key,"hello from java with callback " + i);

			logger.info("Key: " + key);
			// send data
			producer.send(record, (recordMetadata, e) -> {
				// Happens whether record is sent successfully or there's an exception
				if (null == e) {
					logger.info("Received new metadata\n" +
							"Topic: " + recordMetadata.topic() + "\n" +
							"Partition: " + recordMetadata.partition() + "\n" +
							"Offset: " + recordMetadata.offset() + "\n" +
							"Timestamp: " + recordMetadata.timestamp());
				} else {
					System.out.println("ERROR!!!!!!!");
				}
			}).get(); // block.send().  Don't do this in prod
		}


		//flush data
		producer.flush();

		//close producer
		producer.close();
	}

}

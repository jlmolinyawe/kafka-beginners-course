package com.github.jlmolinyawe.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

	private static final String bootstrapServers = "localhost:9092";
	private static final String topicName = "first_topic";
	public static void main(String[] args) {
		System.out.println("HELLO");

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create data
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, "hello from java");

		// send data
		producer.send(record);

		//flush data
		producer.flush();

		//close producer
		producer.close();
	}

}

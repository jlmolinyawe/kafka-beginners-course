package com.github.jlmolinyawe.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

	private static JsonParser jsonParser = new JsonParser();

	public static void main(String[] args) {
		// Create properites
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


		// Create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
		KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);

		filteredStream.to("important_tweets");

		// Build topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

		// Start steams
		kafkaStreams.start();
	}

	private static Integer extractUserFollowersInTweet(String tweetJson) {
		try {
			return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}

	}

}

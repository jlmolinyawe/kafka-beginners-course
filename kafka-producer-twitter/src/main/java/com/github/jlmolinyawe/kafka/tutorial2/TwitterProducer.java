package com.github.jlmolinyawe.kafka.tutorial2;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

	private String consumerKey;
	private String consumerSecret;
	private String token;
	private String secret;
	private String bootstrapServers;

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class);


	List<String> terms = Lists.newArrayList("pokemon", "endgame", "got", "usa");

	private TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	private void loadProperties() {
		try {
			Properties properties = new Properties();
			String fileName = "default.properties";
			InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);

			if (inputStream != null) {
				properties.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + fileName + "' not found in the classpath");
			}

			consumerKey = properties.getProperty("consumerKey");
			consumerSecret = properties.getProperty("consumerSecret");
			token = properties.getProperty("token");
			secret = properties.getProperty("secret");
			bootstrapServers = properties.getProperty("bootstrapServers");
		} catch (Exception e) {
			System.out.println(e);
			System.exit(1);
		}

	}

	private void run() {
		loadProperties();
		logger.info("Start of application");
		BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(100);
		// Create twitter client
		Client client = createTwitterClient(messageQueue);
		client.connect();

		// Create Kafka Producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping application, client and closing producer...");
			client.stop();
			producer.close();
			logger.info("Done!");
		}));

		// Loop to send tweets to Kafka
		while (!client.isDone()) {
			String message = null;
			try {
				message = messageQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}

			if (null != message) {
				logger.info(message);
				producer.send(new ProducerRecord<>("twitter_tweets", null, message), (recordMetadata, e) -> {
					if (null != e)  logger.error("What happened??", e);
				});
			}
		}
		logger.info("End of application");
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// To make producer safe
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		// Not necessary, but good to let others reading the code
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Works for kafka >= v1.1

		// To make this high throughput
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(30*1024)); //32kb batch size

		return new KafkaProducer<>(properties);
	}

	private Client createTwitterClient(BlockingQueue<String> linkedBlockingQueue) {

		/* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
//		List<Long> followings = Lists.newArrayList(1234L, 566788L);
//		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Create client
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(linkedBlockingQueue));
//				.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

		return builder.build();
	}
}

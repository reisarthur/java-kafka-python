package br.cepel.dga;

import java.util.Date;
import java.util.Properties;
import java.security.SecureRandom;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class KafkaUtils {

    public static final String SERVERS = "127.0.0.1:9092";
    public static final String CONSUMER_GROUP_ID = "analysis-listeners";

    public static final String REQUEST_TOPIC = "analysis.request";
    public static final String RESPONSE_TOPIC = "analysis.response";


	public static Properties getConsumerProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
	}

	public static Properties getProducerProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public static void printRecord(ConsumerRecord<String, String> record) {
		System.out.println();
		System.out.println("Key: " + record.key() + ", TimeStamp: " + new Date(record.timestamp()));
		System.out.println("Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset: " + record.offset());
		for (Header recordHeader : record.headers()) {
			System.out.println("Headers: " + recordHeader.key() + " => " + new String(recordHeader.value()));
		}
		System.out.println("Value: " + record.value());
	}

	public static byte[] generateRandomByteArray(int length) {
		SecureRandom random = new SecureRandom();
		byte[] byteArray = new byte[length];
		random.nextBytes(byteArray);
		return byteArray;
	}

}

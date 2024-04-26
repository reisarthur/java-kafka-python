package br.cepel.dga;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

public class AnalysisRequester {

	public static void main(String[] args) {

		KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaUtils.getProducerProperties());

		String topic = KafkaUtils.REQUEST_TOPIC;
		String key = "add";
		String value = "{'data': {'a': 2, 'b': 4}}";

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

		producerRecord.headers().add(new RecordHeader("version", "1.0".getBytes()));

		producer.send(producerRecord);
		producer.flush();
		producer.close();

		System.out.println("Record sent:");
		System.out.println("Key: " + producerRecord.key());
		System.out.println("Value: " + producerRecord.value());

	}

}

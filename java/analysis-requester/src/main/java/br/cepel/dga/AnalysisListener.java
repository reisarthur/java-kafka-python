package br.cepel.dga;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;


public class AnalysisListener {

	public static void main(String[] args) {

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaUtils.getConsumerProperties());

		createShutdownHook(consumer);

		try {

			String topic = KafkaUtils.RESPONSE_TOPIC;
			consumer.subscribe(Arrays.asList(topic));
			System.out.println("Topic subscribed: " + topic);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					KafkaUtils.printRecord(record);
				}
			}

		} catch (WakeupException e) {
			System.out.println("Wake up exception!");
		} catch (Exception e) {
			System.out.println("Unexpected exception: " + e);
		} finally {
			consumer.close();
			System.out.println("The consumer is now gracefully closed.");
		}

	}


	public static void createShutdownHook(KafkaConsumer<String, String> consumer) {
		final Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Detected a shutdown, let's exit by calling consumer.wakeup()...");
				consumer.wakeup();
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

}

package com.cs.kafkatransaction;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample {
	public static void main(String[] args) {
		long noOfMessages = 1000;
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "hello-world-producer");
		producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "trnId");
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer producer = new KafkaProducer(producerConfig);

		producer.initTransactions();
		try {

			producer.beginTransaction();
			for (int i = 0; i < noOfMessages; i++) {
				producer.send(new ProducerRecord<>("inputTopic", "key" + i, "VALUE" + i));

			}
			producer.commitTransaction();
		} catch (KafkaException e) {
			// For all other exceptions, just abort the transaction and try again.
			producer.abortTransaction();
		}
		producer.flush();

		producer.close();
	}

}

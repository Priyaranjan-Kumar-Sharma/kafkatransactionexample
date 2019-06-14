package com.cs.kafkatransaction;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;                                                                                
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class KafkaTransationalExample {
	private static final String CONSUMER_GROUP_ID = "my-group-id";
	private static final String OUTPUT_TOPIC = "output";
	private static final String INPUT_TOPIC = "input";

	public static void main(String args[]) {

		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		KafkaProducer<String, String> producer = createKafkaProducer();

		producer.initTransactions();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(ofSeconds(60));
			if (!records.isEmpty()) {

				producer.beginTransaction();
				List<ProducerRecord<String, String>> outputRecords = processRecords(records);
				for (ProducerRecord<String, String> outputRecord : outputRecords) {
					producer.send(outputRecord);
				}

				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
					long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();

					offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
				}

				producer.sendOffsetsToTransaction(offsetsToCommit, CONSUMER_GROUP_ID);

				producer.commitTransaction();

			}
		}
	}

	private static List<ProducerRecord<String, String>> processRecords(ConsumerRecords<String, String> records) {
		List<ProducerRecord<String, String>> prolist = new ArrayList<ProducerRecord<String, String>>();

		for (ConsumerRecord record : records) {
			System.out.println("consume :Record :::" + record);
			prolist.add(new ProducerRecord<String, String>("output", record.toString()));

		}
		return prolist;
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {
		Properties props = new Properties();
		props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
		props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(singleton(INPUT_TOPIC));
		return consumer;
	}

	private static KafkaProducer<String, String> createKafkaProducer() {

		Properties props = new Properties();
		props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(TRANSACTIONAL_ID_CONFIG, "prod-1");
		props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer(props);

	}
}

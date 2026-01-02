package it.polimi.middleware.kafka.basic_EOS;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class LowerCaseTransactionalForwarder {
	private static final String defaultGroupId = "groupA";
	private static final String defaultInputTopic = "topicA";
	private static final String defaultOutputTopic = "topicB";

	private static final String serverAddr = "localhost:9092";
	//private static final boolean autoCommit = true;
	//private static final int autoCommitIntervalMs = 15000;

	// Default is "latest": try "earliest" instead
	//private static final String offsetResetStrategy = "latest";
	//private static final boolean readUncommitted = true;
	
	
	//private static final String producerTransactionalId = "forwarderTransactionalId";
	 private static final String producerTransactionalId = UUID.randomUUID().toString();

	public static void main(String[] args) {
		// If there are arguments, use the first as group and the second as topic.
		// Otherwise, use default group and topic.
		String groupId = args.length >= 1 ? args[0] : defaultGroupId;
		String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;
		String outputTopic = args.length >= 3 ? args[2] : defaultOutputTopic;

		// Consumer
		final Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); //List of server to which the Consumer connects to.
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); //unique identifier of the consumer group 
		//consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
		//consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
		//consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		//consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList(inputTopic)); //Consumer express interest in a list of topics.
		
		// Producer
		final Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
		producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
		final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
		producer.initTransactions();
		
		Map<String, Integer> timesPerKey = new HashMap<>();
		while (true) {
			final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
			producer.beginTransaction();
			for (final ConsumerRecord<String, String> record : records) {
				String recordKey = record.key();
				if(timesPerKey.containsKey(recordKey)) {
					int value = timesPerKey.get(recordKey)+1;
					timesPerKey.replace(recordKey, value);
				}
				else {
					timesPerKey.put(recordKey,1);
				}
				String processedValue = record.value().toLowerCase(); //Transform in to lower case
				
				producer.send(new ProducerRecord<>(outputTopic, record.key(), timesPerKey.get(recordKey))); // Write in the output topic
				System.out.println(
						"Partition:  " + record.partition() +
						"\tOffset: " + record.offset() +
						"\tRewrite on Topic: " + outputTopic +
						"\tSameKey: " + record.key() +
						"\tProcessedValue: " + processedValue +
						"\tTimesForKey: " + timesPerKey.get(recordKey)
						);
			}
			
			// The producer manually commits the offsets for the consumer within the transaction
	        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
	        for (final TopicPartition partition : records.partitions()) {
	            final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
	            final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
	            map.put(partition, new OffsetAndMetadata(lastOffset + 1));
	        }

	        producer.sendOffsetsToTransaction(map, consumer.groupMetadata()); // Update offset
	        /*
	         * Write in the output topic and update offset in the same transaction --> Exactly One Semantics.
	         */
	        
	        
	        
	        producer.commitTransaction();
			
		}	
	}
}
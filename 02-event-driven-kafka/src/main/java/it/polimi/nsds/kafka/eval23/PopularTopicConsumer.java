package it.polimi.nsds.kafka.eval23;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PopularTopicConsumer {
	private static final String defaultGroupId = "Es2Group";
    private static final String topic = "inputTopic";
    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit)); 
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        
        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        
        final Map<Integer, Integer> partitionCount = new HashMap<>();
        while (true) {
            final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES)); 
            for (final ConsumerRecord<String, Integer> record : records) {
                final String key = record.key();
                final Integer value = record.value();
                final Integer partition = record.partition();
                System.out.println("Received key: " + key + " value: " + value);
                partitionCount.put(partition, partitionCount.getOrDefault(partition, 0) + 1);
                // Compute the partition with the maximum 
                int max = partitionCount.values().stream()
                        .max(Integer::compareTo)
                        .orElse(0);
                partitionCount.entrySet().stream()
                .filter(e -> e.getValue() == max)
                .forEach(System.out::println);
            }
        }
     

    }
}

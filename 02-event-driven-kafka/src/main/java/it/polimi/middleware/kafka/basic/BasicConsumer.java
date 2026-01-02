package it.polimi.middleware.kafka.basic;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {
    private static final String defaultGroupId = "groupA";
    private static final String defaultTopic = "topicA";

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : defaultTopic;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); //List of server to which the Consumer connects to.
        
        /*
         * Notice that if two consumers belong to the same group (they have same groupId) and they are subscribed to the same topic
         * then messages are sent either to one consumer or to the other
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); //unique identifier of the consumer group 
        
        /*
         * AUTO_COMMIT true means that the offsets of read messages are not updated every time we read a new message. Instead they are updated periodically
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit)); // If true the consumer's offset will be periodically committed in the background.
        
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        
        /*
         * When there is a new consumer group that start from scratch:
         * 		earliest: the new consumer group should receive all the messages that are store in kafka in the topic it is interested in.
         * 		latest: returns only messages added after the first poll of the consumer
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic)); //Consumer express interest in a list of topics.
        while (true) {
        	/*
        	 * consumer.poll --> Consumer pulls messages. This method remains blocked until (so it return when):
        	 *   	either there are new record in the list of topics 
        	 *   	or for a maximum duration of time. 
        	 * 
        	 * */
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES)); 
            for (final ConsumerRecord<String, String> record : records) { //iterate over there records
                System.out.print("Consumer group: " + groupId + "\t");
                System.out.println("Partition: " + record.partition() +
                        "\tOffset: " + record.offset() +
                        "\tKey: " + record.key() +
                        "\tValue: " + record.value()
                );
            }
        }
    }
}
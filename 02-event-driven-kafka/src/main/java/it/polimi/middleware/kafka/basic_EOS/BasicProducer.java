package it.polimi.middleware.kafka.basic_EOS;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

// KAFKA DOC: https://kafka.apache.org/documentation/
public class BasicProducer {
    private static final String defaultTopic = "topicA";

    private static final int numMessages = 10000;
    private static final int waitBetweenMsgs = 500;
    private static final boolean waitAck = true;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
    	/*
    	 *  If you run: java BasicProducer → it sends to topicA. --> If there are no arguments, publish to the default topic
    	 *  If you run: java BasicProducer topicX topicY → it will randomly pick one of those topics each time --> Otherwise publish on the topics provided as argument
    	 */
        List<String> topics = args.length < 1 ?
                Collections.singletonList(defaultTopic) :
                Arrays.asList(args);

        final Properties props = new Properties();
        
        /*
         * A list of host/port pairs used to establish the initial connection to the Kafka cluster.
         * Clients use this list to bootstrap and discover the full set of Kafka brokers.
         * While the order of servers in the list does not matter, we recommend including more than one server to ensure resilience if any servers are down.
         * This list does not need to contain the entire set of brokers, as Kafka clients automatically manage and update connections to the cluster efficiently.
         * This list must be in the form host1:port1,host2:port2,....
         * Type:	list
         * Importance:	high
         * Valid Values:	non-null string
         * Default:	""
         */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); //List of server to which the Producer connects to.
        
        
        /*
         * We are serializing keys: Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface.
         * Type:	class	
         * Importance:	high
         */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
        
        /*
         *  We are serializing values: value.serializer Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface.
         *  Type:	class
         *  Importance:	high
         */
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = "Key" + r.nextInt(1);
            final String value = "Val" + i;
            System.out.println(
                    "Topic: " + topic +
                    "\tKey: " + key +
                    "\tValue: " + value
            );

            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            final Future<RecordMetadata> future = producer.send(record); // Metadata will contain information such as partition, topic and offset at which it is added.

            if (waitAck) {
                try {
                	
                	/*
                	 * // the producer waits until Kafka confirms the message was written (blocking call).
                	 * If successful: returns metadata with topic, partition, offset.
                	 * If failed (e.g., broker down): throws ExecutionException.
                	 * 
                	 * Each message is acknowledged by Kafka before sending the next one.
                	 * It’s slower, but guarantees no data loss.
                	 */
                    RecordMetadata ack = future.get(); 
   
                    System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
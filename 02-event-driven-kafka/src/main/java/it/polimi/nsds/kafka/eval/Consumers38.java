package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number:38
// Group members: Vincenzo Del Grosso, Riccardo Scarabelli, Ashkriti Dewan

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
// Yes it is possible: no limit on number of partitions (with the assumption in the file:
						/*You can assume the assignment of keys to partitions to be determinitic
							and identical for any topic that has the same number of partitions
							• You can assume the assignment of partitions to consumers within a
							consumer group to be deterministic and identical for any topic that has
							the same number of partitions
						*/
 
// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
   /*
    * According to the following assumption: "You can assume the assignment of keys to partitions to be deterministic
		and identical for any topic that has the same number of partitions" we need to have the same number of partitions in both topic
    */

// Is it possible to have more than one instance of Merger?
   // Yes, because each instance will be subscribed to both Sensors topics and same key are mapped to same partition (see Assumption on text)
  


// If so, what is the relation between their group id?
	//   If you want to provide parallelism they should be in the same group, otherwise messages would be duplicated

// Is it possible to have more than one partition for topic "merged"?
	// Yes because the Validator will simply forwards each message and does not read data across different partitions.

// Is it possible to have more than one instance of Validator?
	// yes because it is a simple atomic forwarder. So more instance would not cause any problems.


// If so, what is the relation between their group id?
	// If you want to provide parallelism they should be in the same group, otherwise messages would be duplicated

public class Consumers38 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // TODO: add arguments if necessary
        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;
        private static final String outputTopic = "merged";
        private static final String inputTopic1 = "sensors1";
        private static final String inputTopic2 = "sesnors2";
        

        // TODO: add arguments if necessary
        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false)); // we want to commit this offset 

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic1));
            consumer.subscribe(Collections.singletonList(inputTopic2));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            // TODO: implement the processing logic
            
            final Map<String, Integer> sensor1key = new HashMap<>();
            final Map<String, Integer> sensor2key = new HashMap<>();
            while (true) {
            	  final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            	  for (final ConsumerRecord<String, Integer> record : records) {
            		  String key = record.key();
            		  Integer value = record.value();
            		  int sum = 0;
            		  if(record.topic().equals(inputTopic1)) {
            			  sensor1key.put(key, value); //add value or update value
            			  sum = sensor2key.getOrDefault(key, 0) + value;
            		  }
            		  else if(record.topic().equals(inputTopic2)) {
            			  sensor2key.put(key, value); //add value
            			  sum = sensor1key.getOrDefault(key, 0) + value;
            		  }
            		  producer.send(new ProducerRecord<>(outputTopic, key, sum)); 
            		  
                      System.out.println("Partition: " + record.partition() +
                              "\tOffset: " + record.offset() +
                              "\tKey: " + key +
                              "\tValue: " + sum
                      );
                      consumer.commitSync();                   
                  }
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        
        private static final String inputTopic = "merger";
        private static final String outputTopic1 = "output1";
        private static final String outputTopic2 = "output2";

        private static final String producerTransactionalId = UUID.randomUUID().toString();
        

        public Validator(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false)); 
            
            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                	producer.beginTransaction();
                    producer.send(new ProducerRecord<>(outputTopic1, record.key(), record.value()));
                    producer.send(new ProducerRecord<>(outputTopic2, record.key(), record.value()));
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );
                    producer.flush();
                	// The producer manually commits the offsets for the consumer within the transaction
					final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
					map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
					producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
					
					producer.commitTransaction(); 
                }
            }
        }
    }
}
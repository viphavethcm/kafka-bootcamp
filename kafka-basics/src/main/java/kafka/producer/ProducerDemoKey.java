package kafka.producer;

import kafka.KafkaConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKey.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // By default, Partition is a Sticky, It means if we sent a lot of messages at the same time
        // kafka is smart enough to be now batching these messages into 1 batch to just make it more
        // efficient and send to only one partition instead of random
        for (int i = 0;i < 10;i++)
        {
            // create a producer record
            String topic = "checkout_topic";
            String value = "hello "+i;
            String key = "id_" + i;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);

            // send the data - async
            producer.send(producerRecord, (recordMetadata, e) -> {
                // execute every time a record is success or exception is thrown
                if (e == null)
                {
                    log.info("Receive new meta data from "+ recordMetadata.topic() + "\n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Key:" + producerRecord.key() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset:" + recordMetadata.offset() + "\n" +
                            "Timestamp:" + recordMetadata.timestamp());
                }
                else
                {
                    log.error("Error while producing:" ,e);
                }
            });
        }

        // flush data - sync
        // block on this line of code until all the data in producer being sent
        producer.flush();

        // flush and close producer -
        // close is including flush inside
        producer.close();
    }
}

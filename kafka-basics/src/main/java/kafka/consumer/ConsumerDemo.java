package kafka.consumer;

import kafka.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Consumer");

        // none: if no previous offsets are found, then do not event start
        // earliest: read from the very beginning of the topic
        // latest: read only from now

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,KafkaConstant.groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(List.of("checkout_topic"));

        // poll for new data
        // get many records as can.If don't have any reply from Kafka then willing to wait up to 100ms
        // to get some records.If by these 100ms,no records have been received then go to the next line
        // of code and records will be an empty collection
        while (true)
        {
            log.info("Polling...");
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(1000)); //How long willing to wait until move to next line
            for (ConsumerRecord<String,String> record: records)
            {
                log.info("Key:" + record.key() + "-" + "Value:" + record.value());
                log.info("Partition:" + record.partition() + "-" + "Offset:" + record.offset());

            }
        }
    }
}

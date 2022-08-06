package kafka.consumer;

import kafka.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

        public static void main(String[] args) {
        log.info("I'm a Consumer");
        String tmpGroupId = "checkout-consumer-group";

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

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        // wakeup will call consumer to wake up.That means that the next time the consumer is going to
        // do consumer.poll,instead of working is going to throw an exception then it's actually
        // call an wakeup exception and will leave while loop
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in main thread
                // do consumer wake up,but don't stop the running until the main thread finish
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
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
        }catch (WakeupException e)
        {
            log.info("Wake up exception!");
            // we ignore this is an expected exception when closing a consumer
        }catch (Exception e)
        {
            log.error("Unexpected exception");
        }
        finally {
            consumer.close();// this will also commit the offsets if need be
            log.info("The consumer is now gracefully close");
        }
    }
}

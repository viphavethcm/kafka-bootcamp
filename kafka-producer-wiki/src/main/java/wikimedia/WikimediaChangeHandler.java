package wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    KafkaProducer<String,String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String,String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen(){
        // not thing
    }

    @Override
    public void onClosed(){
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        // async
        // will lock to(not async anymore) whenever max.block.ms exception happened
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s) {
        // nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Stream Reading:", t);
    }
}

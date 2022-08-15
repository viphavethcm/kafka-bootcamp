package opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // create an OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // create the index on OpenSearch if it doesn't exist already
        try(openSearchClient; consumer) {
            boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExist)
            {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created");
            }
            else
            {
                log.info("The Wikimedia Index already exists");
            }

            // subscribe a consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true)
            {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received:" + recordCount);

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String,String> record: records)
                {
                    // send record into Open Search

                    // strategies to prevent send duplicate messages into Open Search
                    // strategy 1 : Define an ID using Kafka Record coordinates
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    try{
                        // strategy 2 : Extract id from Json value
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        // add every indexRequest into Bulk Request instead of create each index request and response
                        bulkRequest.add(indexRequest);

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

//                        log.info(response.getId());
                    }catch (Exception e)
                    {

                    }

                }

                if (bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted into Open Search:" + bulkResponse.getItems().length + " records");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offset after the batch is consumed
                    consumer.commitSync();
                    log.info("Offset have been committed");
                }

            }
        }

        // close things


    }

    public static RestHighLevelClient createOpenSearchClient(){
        String server = "https://nt58iyg983:713uz4slyu@kakfa-bootcamp-4993177343.ap-southeast-2.bonsaisearch.net:443";

        // build a URI from server
        RestHighLevelClient restHighLevelClient;
        URI conUri = URI.create(server);

        // extract login information if it exists
        String userInfo = conUri.getUserInfo();

        if (userInfo == null)
        {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(conUri.getHost(),conUri.getPort(),conUri.getScheme())));
        }
        else
        {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0],auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(conUri.getHost(),conUri.getPort(),conUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer()
    {
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,KafkaConstant.groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json)
    {
        // gson library
        return JsonParser
                .parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}

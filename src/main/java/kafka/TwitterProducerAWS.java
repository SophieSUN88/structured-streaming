package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerAWS {

    Logger logger = LoggerFactory.getLogger(TwitterProducerAWS.class.getName());

    //这里填上你们自己的key和token
    final String CONSUMER_KEY = " ";
    final String CONSUMER_SECRET = " ";
    final String TOKEN = " ";
    final String SECRET = " ";

    final String TOPIC = "twitter";
    final String BOOTSTRAP_SERVERS = " ";

    //创建一个Twitter client来帮助我们抓取有效的tweets（这个function不需要记住）
    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        //连接到twitter发送tweet的API
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        //只收集那些tweets里有terms的关键词的
        List<String> terms = Lists.newArrayList("trump", "biden");
        hosebirdEndpoint.trackTerms(terms);

        //用自己的各种token与Twitter API取得连接
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(auth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        return client;
    }

    //创建Kafka producer
    public KafkaProducer<String, String> createKafkaProducer(){

        // 创建producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置config让producer更加可靠
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //同时throughput不会太低

        // 配置config让producer的throughput更高 (throughput vs latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // 创建producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public void run(){

        //用阻塞队列来接收tweets
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // 创建一个Twitter client
        Client client = createTwitterClient(msgQueue);
        // client 与 Twitter endpoint连接
        client.connect();

        // 创建Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // 加上shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down twitter client...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        // producer不停地把这些tweets写入到Kafka里去
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>(TOPIC, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    public static void main(String[] args) {
        new TwitterProducerAWS().run();
    }
}

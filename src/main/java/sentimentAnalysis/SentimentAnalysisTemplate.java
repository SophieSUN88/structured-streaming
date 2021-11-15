package sentimentAnalysis;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class SentimentAnalysisTemplate {

    final String OUTPUT_PATH = "result.csv";
    final String TOPIC = "twitter";
    final String BOOTSTRAP_SERVERS = "localhost:9092";
    final String GROUP_ID = "kafka-demo-consumer";

    //创建consumer
    public KafkaConsumer<String, String> createConsumer(){

        // 配置consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        //配置poll和commit的操作
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 禁止 auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // 创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC));

        return consumer;

    }

    //把Twitter string变成json object
    private JsonObject extractTweetObject(String tweetJson){
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject();
    }

    public void run() throws IOException{
        KafkaConsumer<String, String> consumer = createConsumer();
        Set<String> processedIds = new HashSet<>();
        FileWriter csvWriter = new FileWriter(OUTPUT_PATH);
        csvWriter.append("key_word,tweet,likes,RTs,sentiment\n");

        // 优雅的退出机制
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //调用新的线程去wakeup consumer的thread
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


        try {
            while(true){
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                int recordCount = records.count();

                Set<String> curIds = new HashSet<>();

                for (ConsumerRecord<String, String> record : records){

                    // 两种不同的方法使consumer变成idempotent
                    // 第一种：用Kafka自带的id信息
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // 第二种：用tweet自带的unique id
                    JsonObject tweetObject = extractTweetObject(record.value());
                    String id;
                    try {
                        id = tweetObject.get("id_str").getAsString();
                    } catch (NullPointerException e){
                        continue;
                    }
                    if (processedIds.contains(id)) {
                        continue;
                    }

                    String text = tweetObject.get("text").getAsString()
                            // 移除链接
                            .replaceAll("http.*?[\\S]+", "")
                            // 除去 hashtags
                            .replaceAll("#", "")
                            // 所有的space换成一个space
                            .replaceAll("[\\s]+", " ")
                            // 所有的换行和引号用space替换
                            .replaceAll("[,\"\n]", " ");
                    Properties props = new Properties();
                    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
                    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
                    Annotation annotation = pipeline.process(text);

                    //去计算这句tweet的情感
                    /** 0：very negative
                     *  1：negative
                     *  2：neutral
                     *  3: positive
                     *  4: very positive
                     */

                    int mainSentiment = 0;
                    int longest = 0;
                    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                        Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                        int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                        String partText = sentence.toString();
                        if (partText.length() > longest) {
                            mainSentiment = sentiment;
                            longest = partText.length();
                        }
                    }

                    String key_word = text.toLowerCase().contains("trump")? "Trump": "Biden";
                    int likes = tweetObject.get("favorite_count").getAsInt();
                    int retweets = tweetObject.get("retweet_count").getAsInt();
                    csvWriter.append(key_word + "," + text + "," + likes + "," + retweets + "," + mainSentiment + "\n");
                    curIds.add(id);
                }

                //向Kafka commit offset
                if (recordCount > 0) {
                    csvWriter.flush();
                    consumer.commitSync();
                    processedIds.addAll(curIds);
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            csvWriter.close();
        }
    }


    public static void main(String[] args) throws IOException {
        new SentimentAnalysisTemplate().run();
    }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class testTwitter {

//    Logger logger = LoggerFactory.getLogger(testTwitter.class.getName());

    //这里填上你们自己的key和token
    final String CONSUMER_KEY = " ";
    final String CONSUMER_SECRET = " ";
    final String TOKEN = " ";
    final String SECRET = " ";

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




    public void run() {

        //用阻塞队列来接收tweets
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // 创建一个Twitter client
        Client client = createTwitterClient(msgQueue);
        // client 与 Twitter endpoint连接
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }

    }


    public static void main(String[] args) {
        new testTwitter().run();
    }
}



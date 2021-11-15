package sentimentAnalysis;

import entity.TweetSentiment;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

public class SentimentAnalysisConsole {

    final String TOPIC = "twitter";
    final String BOOTSTRAP_SERVERS = "localhost:9092";

    public Dataset<Row> getStreamingData() {
        SparkSession spark = SparkSession
                .builder()
                .appName("TwitterStreamingApp")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        // 在 structured streaming 中配置kafka相关的config
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("startingOffsets", "earliest")
                .option("subscribe", TOPIC)
                .load()
                .selectExpr("CAST(value AS STRING)");

        return df;
    }

    private StructType createInputSchema() {
        StructType schema = new StructType()
                .add("created_at", DataTypes.StringType, true)
                .add("id", DataTypes.LongType, true)
                .add("text", DataTypes.StringType, true);
        return schema;
    }

    private Dataset<Row> transformDataset(Dataset<Row> input) {
        StructType schema = createInputSchema();
        Dataset<Row> nestedTweets = input
                .select(functions.from_json(new Column("value"), schema).as("data"));
        Dataset<Row> flattenedTweets = nestedTweets
                .selectExpr("data.created_at", "data.id", "data.text");
        return flattenedTweets;
    }


    public void run() throws StreamingQueryException {
        Dataset<Row> input = getStreamingData();
        Dataset<Row> tweets = transformDataset(input);

        Dataset<Row> cleanedTweets = tweets
                .withColumn("text", functions.regexp_replace(tweets.col("text"), "http.*?[\\S]+", " "))
                .filter(tweets.col("id").isNotNull());

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");

        Dataset<TweetSentiment> sentiments = cleanedTweets.map((MapFunction<Row, TweetSentiment>) row -> {
            String text = row.getAs("text");
            long id = row.getAs("id");
            String key_word = text.toLowerCase().contains("trump") ? "Trump" : "Biden";
            int sentiment = UDF.getSentiment(text, props);
            return new TweetSentiment(id, key_word, sentiment);
        }, Encoders.bean(TweetSentiment.class));

        Dataset<Row> analysisReport = sentiments.groupBy("key_word", "sentiment").count();

        //update模式
        StreamingQuery query = analysisReport.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "'false")
                .start();
        query.awaitTermination();
    }



    public static void main(String[] args) throws StreamingQueryException {
        new SentimentAnalysisConsole().run();
    }
}

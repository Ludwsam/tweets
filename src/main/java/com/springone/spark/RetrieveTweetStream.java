package com.springone.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Connection with the Twitter API and Spark streaming to retrieve stream of tweets.
 */
public class RetrieveTweetStream {

  static JavaStreamingContext jssc;

  // WARNING: Change the path and keys
  private static String PATH = "/Users/ludwineprobst/DataSets/twitter/";
  private static String CONSUMER_KEY = "AFiNChb80vxYZfhPls2DXyDpF";
  private static String CONSUMER_SECRET = "JRg7SWyVFkXEESWbEzFzC1xaIGRC3xNdTvrekMvMFk6tjKooOR";
  private static String ACCESS_TOKEN = "493498548-HCCt6LCposCb3Ij7Ygt7ssTxTBPwGoPrnkkDQoaN";
  private static String ACCESS_TOKEN_SECRET = "3pxA3rnBzWa9bmOmOQPWNMpYc4qdOrOdxGFgp6XiCkEKH";

  public static OAuthAuthorization getAuth() {

    return new OAuthAuthorization(
        new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
            .build());
  }

  public static void main(String[] args) {
    // create the spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Play with Spark Streaming")
        .setMaster("local[*]");

    // create a java streaming context and define the window (2 seconds batch)
    jssc = new JavaStreamingContext(conf, Durations.seconds(2));

    System.out.println("Initializing Twitter stream...");

    ObjectMapper mapper = new ObjectMapper();

    JavaDStream<String> tweets = TwitterUtils.createStream(jssc, getAuth())
        .map(tweetStatus ->
            new Tweet(tweetStatus.getLang(),
                tweetStatus.getUser().getId(),
                tweetStatus.getUser().getName(),
                tweetStatus.getText()))
        .map(tweet -> mapper.writeValueAsString(tweet));

    tweets.print();

    tweets.dstream().saveAsTextFiles(PATH, "stream");

    // Start the context
    jssc.start();
    jssc.awaitTermination();
  }

}

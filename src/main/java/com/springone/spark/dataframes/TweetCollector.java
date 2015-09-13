package com.springone.spark.dataframes;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 * To use this class turn your spark version into 1.4.1 or 1.5.0
 */
public class TweetCollector {

    //Collect your tweets using Spark Streaming => Save them as Json file
    //Read Them using Spark SQL/Dataframes => Transform them => Create a simple model => Save the model
    // => Real time prediction via Spark Streaming

    static final String PATH ="/home/samklr/datasets/";
    public static String output = "/home/samklr/code/datasets/tweets/";

    public static SparkContext sc = new SparkContext(
                                                new SparkConf()
                                                        .setMaster("local[*]")
                                                        .setAppName("Spark Musings")
                );

    public static JavaSparkContext jsc = new JavaSparkContext(sc);

    public static JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.minutes(5));

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

    /**
     * Collect the tweets and save them as Json
     */
    public static void collect(){
        String[] filters = {"spring", "springone", "java", "spark" , "#DC", "washington" , "#paris", "donald trump"};

        TwitterUtils.createStream(jssc, getAuth(), filters)
                    .dstream().saveAsTextFiles(output,"tx");

        // Start the context
        jssc.start();
        jssc.awaitTermination();
    }


    /**
     * Turn a stockpile of tweets into tranformed tweets of DataFrame
     * @return
     */
    public static DataFrame tranformer(){

        return null;
    }



    public static void main(String[] args){
        new TweetCollector().collect();
    }

}

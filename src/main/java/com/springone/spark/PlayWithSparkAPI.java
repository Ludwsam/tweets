package com.springone.spark;

import com.springone.spark.utils.Parse;
import com.springone.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "lang":"en",
 *    "name":"Remembrance Day",
 *    "text":"Air Force Upgrade..."}
 *
 *  We want to make some computations on the tweets:
 *  - Find all the persons mentioned on tweets
 *  - Count how many times each person is mentioned
 *  - Find the 10 most mentioned persons by descending order
 */
public class PlayWithSparkAPI {

  // WARNING: Change the path
  // the path file where we stored the data
  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("Load data")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // load the data and create an RDD of Tweet
    JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                              .map(line -> Parse.parseJsonToTweet(line));

    // count the number of tweets
    System.out.println("Number of tweets: " + tweets.count());

    // Find all the persons mentioned on tweets
    JavaRDD<String> mentions = tweets.flatMap(tweet -> Arrays.asList(tweet.getText().split(" ")))
                                     .filter(word -> word.startsWith("@") && word.length() > 1);

    System.out.println("Number of persons mentioned: " + mentions.count());

    // Count how many times each person is mentioned
    JavaPairRDD<String, Integer> mentionCount = mentions.mapToPair(mention -> new Tuple2<>(mention, 1))
                                                        .reduceByKey((x, y) -> x + y);

    System.out.println("first element : " + mentionCount.first());

    // Find the 10 most mentioned persons by descending order
    List<Tuple2<Integer, String>> mostMentioned = mentionCount.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                                                              .sortByKey(false)
                                                              .take(10);
    System.out.println("10 most mentioned people");
    for (Tuple2 ele: mostMentioned) {
      System.out.println(ele);
    }
  }

}

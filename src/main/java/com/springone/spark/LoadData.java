package com.springone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 *  Load tweets using Spark SQL.
 *  Define how many different languages there are.
 */
public class LoadData {

  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    SparkConf conf = new SparkConf()
        .setAppName("fef")
        .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL.
    // It is the entry point in Spark SQL.
    SQLContext sqlContext = new SQLContext(sc);

    // load the data (json file here) and register the data in the "tweets" table.
    DataFrame tweets = sqlContext.jsonFile(pathToFile);
    tweets.registerTempTable("tweets");

    System.out.println("tweets.count() " + tweets.count());

    // Define the different language found.
    // We need to know that to identify how many cluster we need in the K-means algorithm.
    DataFrame result = sqlContext.sql("SELECT lang, COUNT(*) as cnt FROM tweets GROUP BY lang ORDER BY cnt DESC");

    //result.show();
    System.out.println("cluster number: " + result.collectAsList().size());

    System.out.println(result.collectAsList());
  }

}


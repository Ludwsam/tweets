package com.springone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from Spark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In here we create a dataframe with the content of a JSON file.
 *
 */
public class LoadData {

  // WARNING: Change the path
  // the path file where we stored the data
  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("Load data")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL / Dataframe.
    // It is the entry point in Spark SQL.
    SQLContext sqlContext = new SQLContext(sc);

    // load the data (json file here) and register the data in the "tweets" table.
    DataFrame tweets = sqlContext.jsonFile(pathToFile);
    tweets.registerTempTable("tweets");

    // Displays the content of the DataFrame to stdout
    tweets.show();
    // we see something like that:
    // id         lang        name                 text
    // 632952234  en          Remembrance Day      Air Force Upgrade...

    // filter tweets in japanese
    DataFrame filtered = tweets.filter(tweets.col("lang").equalTo("ja")).toDF();
    filtered.show();
    // we see something like that:
    // id         lang        name                 text
    // 2907381456 ja          なっちゃん            クナイ投げた方がいいか　投げまくるか

    // Count the tweets for each language
    // It will be use to determine the number of clusters we want for the K-means algorithm.
    DataFrame result = sqlContext.sql("SELECT lang, COUNT(*) as cnt FROM tweets GROUP BY lang ORDER BY cnt DESC");

    System.out.println("number of languages: " + result.collectAsList().size());
    System.out.println(result.collectAsList());

  }

  /** en,3071
      ja,1221
      es,778
      ar,609
      pt,540
      und,529
      tr,260
      ru,223
      fr,209
      in,182
      ko,154
      th,118
      it,77
      tl,67
      pl,59
      de,56
      nl,38
   */
}


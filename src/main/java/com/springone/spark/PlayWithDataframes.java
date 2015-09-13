package com.springone.spark;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * This requires Spark 1.4 +
 */
public class PlayWithDataframes {
    final static Logger log = Logger.getLogger(PlayWithDataframes.class);


    private static String PATH = "/home/samklr/code/datasets/*/*";

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setAppName("Dataframes && ML-Lib")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL / Dataframe.
        // It is the entry point in Spark SQL.
        SQLContext sqlContext = new SQLContext(sc);

        // load the data (json file here) and register the data in the "tweets" table.
        DataFrame tweets = sqlContext.read().json(PATH);  // You can also read different type of files :Parquet, ORC, Web APIs, etc .

        tweets.printSchema();

        log.info("Number of Tweets " + tweets.count());

        // Displays the content of the DataFrame to stdout
        tweets.show(5);

        //That's a bit noisy. Let's just display the fields we're interested on : status, languages and handler

        //Register the DataFrame as SQL Table on which we can query.
        tweets.registerTempTable("tweets");

        DataFrame tweets2 = sqlContext.sql("SELECT lang, user.screenName, text FROM tweets ");
        tweets2.show();
        // we see something like that:
        //        lang        name                 text
        //      en          Remembrance Day      Air Force Upgrade...

        // filter tweets only in english, french and spanish
        // and do some cleaning. I will remove all the Rows that are not correct or contains null or N/A values by using na().drop()

        DataFrame filtered = tweets2.filter(       ( tweets.col("lang").equalTo("en"))
                                                 .or(tweets.col("lang").equalTo("fr"))
                                                 .or(tweets.col("lang").equalTo("es")))
                                    .na().drop();

        filtered.show();

        log.info("Filtered Tweet count " + filtered.count());


        // Now Let's do some machine learning with these tweets



    }
}

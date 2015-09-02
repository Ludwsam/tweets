package com.springone.spark;


import com.springone.spark.utils.NGram;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Use the Kmeans method to define cluster per language.
 */
public class KmeanModel {

  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("K-means")
        .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    // need to build the data :D :D
    DataFrame tweets = sqlContext.jsonFile(pathToFile);
    tweets.registerTempTable("tweets");

    DataFrame dataFrame = sqlContext.sql("SELECT text FROM tweets WHERE lang in ('en', 'es', 'ar', 'pt', 'ja')");
    JavaRDD<String> result = dataFrame.javaRDD().map(row -> row.toString());

    System.out.println("first element of the sql request : " + result.first());
    System.out.println("sql request count : " + result.count());

    // remove some special caracters...url, # and @ mentions
    // http://stackoverflow.com/questions/161738/what-is-the-best-regular-expression-to-check-if-a-string-is-a-valid-url
    // TODO
    JavaRDD<String> points = result
        .map(e -> e.toLowerCase())
        .map(e -> e.replaceAll("rt:\\w+", ""))
        .map(e -> e.replaceAll("\\s+#\\w+", ""))
        .map(e -> e.replaceAll("#\\w+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)://[\\w/%.-]+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", ""))
        .map(e -> e.replaceAll("\\s+@\\w+", ""))
        .map(e -> e.replaceAll("@\\w+", ""))
        .filter(e -> e.length() > 80);

    System.out.println("Point first: " + points.first());
    System.out.println("Point take: " + points.take(10));
    System.out.println("count " + points.count());

    List<String> tests = points.take(100);

    // slip into 2-gram
    JavaRDD<Iterable<String>> lists = points.map(ele -> NGram.ngrams(2, ele));

    System.out.println("With ngram: " + lists.first());

    // https://en.wikipedia.org/wiki/Feature_hashing
    HashingTF hash = new HashingTF(1000);
    RDD<Vector> vectors = lists.map(line -> hash.transform(line)).rdd().cache();

    System.out.println("Vectors count: " + vectors.count());

    int clusterNumber = 5;
    int iter = 20;

    KMeansModel model = KMeans.train(vectors, clusterNumber, iter);

    // TODO improve the error
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    //double wssse = model.computeCost(vectors);
    //System.out.println("Within Set Sum of Squared Errors = " + wssse);

    List<Vector> examples = vectors.toJavaRDD().take(100);

    for (String test: tests) {
      Iterable<String> ngram = NGram.ngrams(2, test);
      Vector v = hash.transform(ngram);
      int cluster = model.predict(v);
      System.out.println(test + " is in the cluster " + cluster);
    }


    // TODO analyze the results
    //for (Vector example: examples) {
      //System.out.println(example + " is in the cluster " + model.predict(example));
    //}

  }
}

package com.springone.spark;


import com.springone.spark.utils.NGram;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

// TODO finish this part. It doesn't work right now
public class NaivBayesModel {

  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("naives bayes")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    // need to build the data :D :D
    DataFrame tweets = sqlContext.jsonFile(pathToFile);
    tweets.registerTempTable("tweets");

    DataFrame dataFrame = sqlContext.sql("SELECT lang, text FROM tweets WHERE lang in ('en', 'es', 'ja')");
    JavaRDD<String[]> result = dataFrame.javaRDD().map(row -> new String[]{row.get(0).toString(), row.get(1).toString()});

    System.out.println("sql request : " + result.first()[0] + " " + result.first()[1]);

      // build the labeled points
      // the language (lang) is the point. We need to convert it into Double
      // en <- 0
      // es <- 1
      // ja <- 2
    /*JavaRDD<String[]> points = result
        .map(ele -> {
          Object[] point = new Object[2];
          Double label = 0.0;

          switch (ele[0]) {
            case "en":
              label = 0.0;
              break;
            case "es":
              label = 1.0;
              break;
            case "ja":
              label = 2.0;
              break;
            default:
              label = -1.0;
          }

          point[0] = label;
          point[1] = ele[1];

          return point;
        })
        .map(e -> new String[]{(String) e[0], ((String) e[1]).replaceAll("#\\w+", "")})
        .map(e -> new String[]{e[0], e[1].replaceAll("(?:https?|http?)://[\\w/%.-]+", "")})
        .map(e -> new String[]{e[0], e[1].replaceAll("@\\w+", "")});

    System.out.println("Point first: " + points.first().length + " " + points.first()[0] + " " + points.first()[1]);

    //JavaRDD<Tuple2<String, Iterable<String>>> lists = points.map(ele -> new Tuple2<>(ele[0], NGram.ngrams(2, ele[1])));

    //System.out.println("With ngram: " + lists.first()._1() + " " + lists.first()._2());

    /*System.out.println(points.first().length);
    System.out.println(points.first()[0]);
    System.out.println(points.first()[1]);
    System.out.println(points.first()[2]);
    System.out.println(points.first()[3]);
    System.out.println(points.first()[4]);
    System.out.println(points.first()[5]);
    System.out.println(points.first()[6]);*/

    //RDD<LabeledPoint> input = null;


    //NaiveBayesModel model = NaiveBayes.train(input);

  }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by nasir on 9/12/15.
 */
public class Application {

    public static void main(String [] args) {

        String logFile = "PATH TO TEXT FILE TO BE READ";

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        //Java 7
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        //Java 7
        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        //Java 8
        long numAsJava8 = logData.filter(s -> s.contains("a")).count();

        //Java 8
        long numBsJava8 = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAsJava8 + ", lines with b: " + numBsJava8);

    }
}

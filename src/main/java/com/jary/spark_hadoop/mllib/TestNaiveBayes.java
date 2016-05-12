package com.jary.spark_hadoop.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class TestNaiveBayes {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
	    SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local[2]");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    JavaRDD<String> data = sc.textFile("/tmp/spark/1.txt");
	    RDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {

			public LabeledPoint call(String line) throws Exception {
				String[] parts = line.split(",");
		        String[] strArr = parts[1].split(" ");
		        double values[] = new double[strArr.length];
		        for (int i = 0; i < strArr.length; i++) {
		        	values[i] = Double.parseDouble(strArr[i]);
				}
		        //LabeledPoint代表一条训练数据，即打过标签的数据
		        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
			}
	    	
		}).rdd();
	 
	    //分隔为两个部分，60%的数据用于训练，40%的用于测试
	    RDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.6, 0.4}, 11L);
	    JavaRDD<LabeledPoint> training = splits[0].toJavaRDD();
	    JavaRDD<LabeledPoint> test = splits[1].toJavaRDD();
	 
	    //训练模型， Additive smoothing的值为1.0（默认值）
	    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
	 
	    JavaRDD<Double> prediction = test.map(new Function<LabeledPoint, Double>() {

			public Double call(LabeledPoint p) throws Exception {
				return model.predict(p.features());
			}
		});
	    JavaPairRDD<Double, Double> predictionAndLabel = prediction.zip(test.map(new Function<LabeledPoint, Double>() {

			public Double call(LabeledPoint v1) throws Exception {
				return v1.label();
			}
		}));
	    //用测试数据来验证模型的精度
	    double accuracy = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double,Double>, Boolean>() {
			public Boolean call(Tuple2<Double, Double> v1) throws Exception {
				return v1._1().equals(v1._2());
			}
		}).count() / test.count();
	    System.out.println("Accuracy=" + accuracy);
	    //预测类别
	    System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[]{0.5, 3.0, 0.5})));
	    System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{1.5, 0.4, 0.6})));
	    System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[]{0.3, 0.4, 2.6})));
	}
}

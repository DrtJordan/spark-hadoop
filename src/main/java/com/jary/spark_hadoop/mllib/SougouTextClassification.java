package com.jary.spark_hadoop.mllib;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import cn.hadoop.spark_hadoop.util.MLConstants;
import scala.Tuple2;

public class SougouTextClassification {

	public static void main(String[] args) {
		if(args.length < 2){
			System.err.println("Usage: SougouTextClassification <trainFile> <filePath>");
			System.exit(1);
		}
		String trainFile = args[0];
		String filePath = args[1];
		
	    SparkConf sparkConf = new SparkConf().setAppName("SougouTextClassification");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new SQLContext(jsc);
	    //训练语料
	    JavaRDD<LabeledPoint> training = jsc.objectFile(trainFile);
	    //要分类的文档RDD
	    JavaRDD<LabeledPoint> test = convertDataRDD(jsc, sqlContext, filePath);
	    //开始文档分类
	    doClassification(training, test);
	}
	/**
	 * 文档分类
	 * @param training 训练语料RDD
	 * @param test 文档RDD
	 */
	private static void doClassification(JavaRDD<LabeledPoint> training,JavaRDD<LabeledPoint> test){
	    //训练模型， Additive smoothing的值为1.0（默认值）
	    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
	 
	    JavaRDD<Double> prediction = test.map(new Function<LabeledPoint, Double>() {

			public Double call(LabeledPoint p) throws Exception {
				return model.predict(p.features());
			}
		});
	    
	    JavaPairRDD<Double, Integer> matchCounts = prediction.mapToPair(new PairFunction<Double, Double, Integer>() {

			public Tuple2<Double, Integer> call(Double t) throws Exception {
				return new Tuple2<Double, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
	    List<Tuple2<Double, Integer>> list = matchCounts.collect();
	    for (Tuple2<Double, Integer> tuple2 : list) {
			System.out.println(tuple2._1 + " " + tuple2._2 + " " + test.count() + " " + ((tuple2._2+0.0) / test.count()));
		}
	}
	static Participle participle = new IkParticiple();
	private static JavaRDD<LabeledPoint> convertDataRDD(JavaSparkContext jsc,SQLContext sqlContext,String filePath){
		JavaRDD<String> data = jsc.textFile(filePath);
		JavaRDD<Row> jrdd = data.map(new Function<String, Row>() {

			public Row call(String v1) throws Exception {
				String words[] = participle.participle(v1, false);
				String value = "";
				for (String w : words) {
					value+=(w+" ");
				}
		        return  RowFactory.create(0.0,value);
			}
			
		});
		StructType schema = new StructType(new StructField[] {
				new StructField("label", DataTypes.DoubleType, false,
						Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false,
						Metadata.empty()) });
		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence")
				.setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);
		
		HashingTF hashingTF = new HashingTF().setInputCol("words")
				.setOutputCol("rawFeatures").setNumFeatures(MLConstants.NUM_FEATURES);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);
		
		JavaRDD<LabeledPoint> trainDataRDD = rescaledData.select("features", "label").javaRDD().map(new Function<Row, LabeledPoint>() {

			public LabeledPoint call(Row row) throws Exception {
				SparseVector features = row.getAs(0);
		        //LabeledPoint代表一条训练数据，即打过标签的数据
		        return new LabeledPoint(row.getDouble(1), Vectors.dense(features.toArray()));
			}
	    	
		});
		return trainDataRDD;
	}
}

package com.jary.spark_hadoop.mllib;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
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

import cn.hadoop.spark_hadoop.domain.Keyword;
import cn.hadoop.spark_hadoop.util.DBHelper;
import cn.hadoop.spark_hadoop.util.MLConstants;

public class TrainKeyword implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6669289134376540221L;
	
	private static Logger logger = Logger.getLogger(TrainKeyword.class);

	private static final String select_sql = "select keyword, tag_id from t_keyword where 1=1 ";
	
	private static final String SEPARATOR_NL = System.getProperty("line.separator");
	
	private static final Participle participle = new IkParticiple();
	
	public static void main(String[] args) {
//		args = new String[2];
//		args[0] = "/tmp/spark/keyword/train/";
		if(args.length < 1){
			System.err.println("Usage: TrainDataRecord <savePath>");
			System.exit(1);
		}
		String savePath = args[0];
		SparkConf sparkConf = new SparkConf().setAppName("TrainDataRecord");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		List<Keyword> list = getKeywordList();
		JavaRDD<Keyword> javaRDD = jsc.parallelize(list);
		TrainKeyword trainKeyword = new TrainKeyword();
		JavaRDD<LabeledPoint> training = trainKeyword.trainDataRDD(jsc, sqlContext, javaRDD);
		//序列化RDD对象
		training.saveAsObjectFile(savePath);
		
	}
	
	/**
	 * 关键词
	 * 
	 * @return
	 */
	private static List<Keyword> getKeywordList() {
		List<Keyword> keywordList = new ArrayList<Keyword>();
		ResultSet resultSet = DBHelper.executeQuery(select_sql);
		try {
			while (resultSet.next()) {
				Keyword keyword = new Keyword();
				keyword.setTagId(resultSet.getInt("tag_id"));
				keyword.setKeyword(resultSet.getString("keyword"));
				keywordList.add(keyword);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return keywordList;
	}
	
	private JavaRDD<LabeledPoint> trainDataRDD(JavaSparkContext jsc,SQLContext sqlContext,JavaRDD<Keyword> javaRDD){
		JavaRDD<Row> jrdd = javaRDD.map(new Function<Keyword, Row>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Keyword arg0) throws Exception {
				String words[] = participle.participle(arg0.getKeyword(), true);
				String value = "";
				for (String w : words) {
					value+=(w+" ");
				}
		        return  RowFactory.create(arg0.getTagId(), value);
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
		
		//计算每个词的TF-IDF
		HashingTF hashingTF = new HashingTF().setInputCol("words")
				.setOutputCol("rawFeatures").setNumFeatures(MLConstants.NUM_FEATURES);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		Row[] words = featurizedData.select("label", "words", "rawFeatures").take(100);
		System.out.println("当前分词，词频排行：");
		for (Row row : words) {
			System.out.println(row.toString());
		}
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")/*.setMinDocFreq(10)*/;
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

package com.jary.spark_hadoop.novel;


import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
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

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.summary.TextRankKeyword;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
import com.jary.spark_hadoop.mllib.IkParticiple;
import com.jary.spark_hadoop.mllib.Participle;
import com.jary.spark_hadoop.mllib.TrainDataRecord;

import cn.hadoop.spark_hadoop.util.FileUtils;
import cn.hadoop.spark_hadoop.util.HdfsFileSystemUtil;
import cn.hadoop.spark_hadoop.util.MLConstants;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

/**
 * 小说文本分析
 * @author hjl
 * @date 2016年9月1日 下午1:19:44 
 */
public class TextAnalysis {
	//D:\tmp\bookcache\1520160001\1
	public static String localPath = "D:/tmp/spark/bookcache/001/01/";
	public static String hdfsPath = "/tmp/spark/bookcache/1520160001/1/";

	static Participle participle = new IkParticiple();
	
	
	public static void main(String[] args) throws Exception {
//		System.setProperty("spark.executor.memory", "3g");
//
//	    SparkConf sparkConf = new SparkConf().setAppName("TextAnalysis").setMaster("local");
//	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//	    SQLContext sqlContext = new SQLContext(jsc);
//		JavaRDD<LabeledPoint> training = trainDataRDD(jsc, sqlContext, localPath);
		
//		String content = FileUtils.getContent("D:/tmp/spark/bookcache/1520160000/0/1005000120.txt", "utf-8");
		
//	    String content = "程序员(英文Programmer)是从事程序开发、维护的专业人员。一般将程序员分为程序设计人员和程序编码人员，但两者的界限并不非常清楚，特别是在中国。软件从业人员分为初级程序员、高级程序员、系统分析员和项目经理四大类。";
//	    List<String> keywordList = HanLP.extractKeyword(content, 20);
//	    System.out.println(keywordList);
		
		test2();
		//删除停词缓存文件
//		File file = new File("D:/tmp/HanLP/data/dictionary/stopwords.txt.bin");
//		file.delete();
		
	}
	
	private static JavaRDD<LabeledPoint> trainDataRDD(JavaSparkContext jsc,SQLContext sqlContext,String trainPath){
		JavaPairRDD<String,String> data = jsc.wholeTextFiles(trainPath);
		JavaRDD<Row> jrdd = data.map(new Function<Tuple2<String,String>, Row>() {
	
			public Row call(Tuple2<String, String> v1) throws Exception {
				String[] s = v1._1.split("/");
				double num = Double.parseDouble(s[s.length - 1].substring(0, s[s.length - 1].lastIndexOf(".")));
				
				String words[] = participle.participle(v1._2, true);
				String value = "";
				for (String w : words) {
					value+=(w+" ");
				}
		        return  RowFactory.create(num,value);
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
//		Row[] words = featurizedData.select("sentence", "words", "rawFeatures").take(10);
//		System.out.println("当前分词，词频排行1：");
//		for (Row row : words) {
//			System.out.println(row.toString());
//		}
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")/*.setMinDocFreq(10)*/;
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);
		
//		ChiSqSelector selector = new ChiSqSelector()
//			      .setNumTopFeatures(10)
//			      .setFeaturesCol("features")
//			      .setLabelCol("label")
//			      .setOutputCol("selectedFeatures");
//
//	    DataFrame result = selector.fit(rescaledData).transform(rescaledData);
//	    result.show();

		Row[] words2 = rescaledData.select("words", "features").collect();
		System.out.println("当前分词，词频排行2：");
		for (Row row : words2) {
			WrappedArray wrappedArray = (WrappedArray) row.get(0);
			SparseVector sparseVector = (SparseVector) row.get(1);
			int[] features0 = sparseVector.indices();
			double[] features = sparseVector.values();
			System.out.println("关键词数："+wrappedArray.length() + ", 特征向量:" + features0.length + ", tf-idf:" + features.length);
			System.out.println(row.toString());
		}
		
		JavaRDD<LabeledPoint> trainDataRDD = rescaledData.select("features", "label").javaRDD().map(new Function<Row, LabeledPoint>() {

			public LabeledPoint call(Row row) throws Exception {
				SparseVector features = row.getAs(0);
		        //LabeledPoint代表一条训练数据，即打过标签的数据
		        return new LabeledPoint(row.getDouble(1), Vectors.dense(features.toArray()));
			}
	    	
		});
		return trainDataRDD;
	}
	
	
	/**
	 * 普通分词，计算词频
	 * @throws Exception
	 */
	public void test() throws Exception {
		Map<String, Integer> keywordMap = new HashMap<String, Integer>();
		File fileD = new File(localPath);
		File[] fileList = fileD.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			File file = fileList[i];
			String content = FileUtils.getContent(file, "utf-8");
			String words[] = participle.participle(content, true);
			for (int j = 0; j < words.length; j++) {
				String keyword = words[j];
		    	Integer count = keywordMap.get(keyword);
		    	if (count==null) {
		    		count = 0;
				}
		    	count++;
		    	keywordMap.put(keyword, count);
			}
		}
		List<Map.Entry<String, Integer>> mappingList = null;
		//通过ArrayList构造函数把map.entrySet()转换成list
		mappingList = new ArrayList<Map.Entry<String, Integer>>(keywordMap.entrySet());
		//通过比较器实现比较排序
		Collections.sort(mappingList, new Comparator<Map.Entry<String, Integer>>(){
			public int compare(Map.Entry<String, Integer> mapping1,Map.Entry<String, Integer> mapping2){
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});
		for(Map.Entry<String, Integer> mapping:mappingList){
			if (mapping.getValue()<=300) {
				break;
			}
			System.out.print(mapping.getKey() + ":" + mapping.getValue()/**1.0/fileList.length*/ + ", ");
		}
	}
	
	
	/**
	 * HanLP分词、停词后，提取关键词
	 * @throws Exception
	 */
	public static void test1() throws Exception {
		Map<String, Integer> keywordMap = new HashMap<String, Integer>();
		File fileD = new File(localPath);
		File[] fileList = fileD.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			File file = fileList[i];
			String content = FileUtils.getContent(file, "utf-8");
			HanLP.extractKeyword(content, 20);
//			TextRankKeyword textRankKeyword = new TextRankKeyword();
//			NotionalTokenizer notionalTokenizer = new NotionalTokenizer();
//	        textRankKeyword.setSegment(notionalTokenizer);
//	        List<String> keywordList = textRankKeyword.getKeyword(content);
		    List<String> keywordList = TextRankKeyword.getKeywordList(content, 50);
		    for (String keyword : keywordList) {
		    	Integer count = keywordMap.get(keyword);
		    	if (count==null) {
		    		count = 0;
				}
		    	count++;
		    	keywordMap.put(keyword, count);
			}
		}
		List<Map.Entry<String, Integer>> mappingList = null;
		//通过ArrayList构造函数把map.entrySet()转换成list
		mappingList = new ArrayList<Map.Entry<String, Integer>>(keywordMap.entrySet());
		//通过比较器实现比较排序
		Collections.sort(mappingList, new Comparator<Map.Entry<String, Integer>>(){
			public int compare(Map.Entry<String, Integer> mapping1,Map.Entry<String, Integer> mapping2){
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});
		List<String> keywordList = new ArrayList<String>();
		for(Map.Entry<String, Integer> mapping:mappingList){
			if (mapping.getValue()<=19) {
				break;
			}
//			System.out.println(mapping.getKey());
			keywordList.add(mapping.getKey());
		}
		Collections.sort(keywordList, Collator.getInstance(java.util.Locale.CHINA));
		for (String keyword : keywordList) {
			System.out.println(keyword);
		}
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
	}
	
	
	/**
	 * HanLP分词、停词后，提取关键词及词频
	 * @throws Exception
	 */
	public static void test2() throws Exception {
		Map<String, Integer> keywordMap = new HashMap<String, Integer>();
		File fileD = new File(localPath);
		File[] fileList = fileD.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			File file = fileList[i];
			String content = FileUtils.getContent(file, "utf-8");
			HanLP.extractKeyword(content, 20);
//			TextRankKeyword textRankKeyword = new TextRankKeyword();
//			NotionalTokenizer notionalTokenizer = new NotionalTokenizer();
//	        textRankKeyword.setSegment(notionalTokenizer);
//	        List<String> keywordList = textRankKeyword.getKeyword(content);
		    List<String> keywordList = TextRankKeyword.getKeywordList(content, 50);
		    for (String keyword : keywordList) {
		    	Integer count = keywordMap.get(keyword);
		    	if (count==null) {
		    		count = 0;
				}
		    	count++;
		    	keywordMap.put(keyword, count);
			}
		}
		List<Map.Entry<String, Integer>> mappingList = null;
		//通过ArrayList构造函数把map.entrySet()转换成list
		mappingList = new ArrayList<Map.Entry<String, Integer>>(keywordMap.entrySet());
		//通过比较器实现比较排序
		Collections.sort(mappingList, new Comparator<Map.Entry<String, Integer>>(){
			public int compare(Map.Entry<String, Integer> mapping1,Map.Entry<String, Integer> mapping2){
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});
		List<String> keywordList = new ArrayList<String>();
		for(Map.Entry<String, Integer> mapping:mappingList){
			if (mapping.getValue()<=19) {
				break;
			}
			System.out.print(mapping.getKey() + ": " + mapping.getValue() + ", ");
		}
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
	}

	
}

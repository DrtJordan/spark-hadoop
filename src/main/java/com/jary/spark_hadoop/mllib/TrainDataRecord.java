package com.jary.spark_hadoop.mllib;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

import scala.Tuple2;

public class TrainDataRecord implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6669289134376540221L;
	private static Logger logger = Logger.getLogger(TrainDataRecord.class);
	private static final String SEPARATOR_NL = System.getProperty("line.separator");
	private static final Participle participle = new IkParticiple();
	
/*	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if(args.length < 3){
			System.err.println("Usage: TrainDataRecord <inFile> <outFile> <category>");
			System.exit(1);
		}
		String inPath = args[0];
		String outPath = args[1];
		final String category = args[2];
		SparkConf sparkConf = new SparkConf().setAppName("TrainDataRecord");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		final Participle participle = new IkParticiple();
		//返回key-value，key=文件路径，value=每行文本数据
		JavaPairRDD<String,String> data = jsc.wholeTextFiles(inPath);
		JavaRDD<Row> jrdd = data.map(new Function<Tuple2<String,String>, Row>() {

			public Row call(Tuple2<String, String> v1) throws Exception {
				String[] s = v1._1.split("/");
				double num = Double.parseDouble(s[s.length - 2].substring(1));
				
				String words[] = participle.participle(v1._2, false);
				String value = "";
				for (String w : words) {
					value+=(w+" ");
				}
		        return  RowFactory.create(num,v1._2.split(",")[1]);
			}
			
		});
		JavaRDD<String> data = jsc.textFile(inPath);
		JavaRDD<Row> jrdd = data.map(new Function<String, Row>() {

			public Row call(String v1) throws Exception {
				double num = Double.parseDouble(category);
				
				String words[] = participle.participle(v1, false);
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
		
		int numFeatures = 500000;
		HashingTF hashingTF = new HashingTF().setInputCol("words")
				.setOutputCol("rawFeatures").setNumFeatures(numFeatures);
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
		
		//分隔为两个部分，60%的数据用于训练，40%的用于测试
	    RDD<LabeledPoint>[] splits = trainDataRDD.randomSplit(new double[]{0.6, 0.4}, 11L);
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
	    System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{0, 0.4054651081081644, 0.4054651081081644})));
	    System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{0.4054651081081644,0, 0.4054651081081644})));
	    //预测类别
	    System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[]{0.5, 3.0, 0.5})));
	    System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{1.5, 0.4, 0.6})));
	    System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[]{0.3, 0.4, 2.6})));
		
		try {
			File file = new File(outPath);
			if( file.exists() ){
				logger.error("output file already exists:"+outPath);
				return;
			}else{
				file.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
			
			List<LabeledPoint> resultList = trainDataRDD.collect();
			for (LabeledPoint labeledPoint : resultList) {
				String idfValues = Arrays.toString(labeledPoint.features().toArray()).replace(",", "").replace("[", "").replace("]", "");
				if( !idfValues.equals("0") ){
					writer.write(String.format("%s,%s"+SEPARATOR_NL,labeledPoint.label(),idfValues));
				}
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}*/
	public static void main(String[] args) {
		if(args.length < 2){
			System.err.println("Usage: TrainDataRecord <trainPath> <savePath>");
			System.exit(1);
		}
		String trainPath = args[0];
		String savePath = args[1];
		SparkConf sparkConf = new SparkConf().setAppName("TrainDataRecord");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		TrainDataRecord dataRecord = new TrainDataRecord();
		JavaRDD<LabeledPoint> training = dataRecord.trainDataRDD(jsc, sqlContext, trainPath);
		//序列化RDD对象
		training.saveAsObjectFile(savePath);
		
	}
	private JavaRDD<LabeledPoint> trainDataRDD(JavaSparkContext jsc,SQLContext sqlContext,String filePath){
		JavaPairRDD<String,String> data = jsc.wholeTextFiles(filePath);
		JavaRDD<Row> jrdd = data.map(new Function<Tuple2<String,String>, Row>() {
	
			public Row call(Tuple2<String, String> v1) throws Exception {
				String[] s = v1._1.split("/");
				double num = Double.parseDouble(s[s.length - 2].substring(1));
				
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
		
		int numFeatures = 50000;
		//计算每个词的TF-IDF
		HashingTF hashingTF = new HashingTF().setInputCol("words")
				.setOutputCol("rawFeatures").setNumFeatures(numFeatures);
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

package com.jary.spark_hadoop.mllib;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import cn.hadoop.spark_hadoop.dao.UrlTxtPathDao;
import cn.hadoop.spark_hadoop.domain.UrlLib;
import cn.hadoop.spark_hadoop.domain.UrlTxtPath;
import cn.hadoop.spark_hadoop.util.FileUtils;
import cn.hadoop.spark_hadoop.util.HdfsFileSystemUtil;
import cn.hadoop.spark_hadoop.util.JsoupUtil;
import cn.hadoop.spark_hadoop.util.MLConstants;
import cn.hadoop.spark_hadoop.util.UrlLibUtil;
import scala.Tuple2;

/**
 * @author ll
 * @date 2016年5月4日 上午10:08:33
 */
public class AutoUrlLibClassification {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(AutoUrlLibClassification.class);

	public static final String trainFile = "/tmp/spark/20160823/train/";

	public static final int AD_CATEGORY_UNKNOWN_PUSH = 0;// 未知

	public static final long TIME_INTERVAL = 1000 * 60 * 10;//时间间隔

	public static final long TIME_STOP = 1000 * 60 * 3;//时间间隔
	
//	private static final Map<String,Integer> lineCountMap = new ConcurrentHashMap<String,Integer>();
	
	/**
	 * 失效的url
	 */
	private static final List<String> invalidUrl = new ArrayList<>();

	public static void main(String[] args) {
		
		if(args.length < 1){
			System.err.println("Usage: SougouTextClassification <trainFile>");
			System.exit(1);
		}
		String trainFile = args[0];
//		String filePath = args[1];
		
		long start = new Date().getTime();
		logger.info("##########start（" + start + "）.............");
	    
		//查询待处理的txt路径
		UrlTxtPath urlTxtPath = UrlTxtPathDao.queryByStatus(UrlTxtPathDao.STATUS_1);
		if (urlTxtPath!=null && !StringUtils.isEmpty(urlTxtPath.getPath())) {
			UrlTxtPathDao.updateStatus(urlTxtPath.getId(), UrlTxtPathDao.STATUS_2);

			SparkConf sparkConf = new SparkConf().setAppName("AutoUrlLibClassification");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			SQLContext sqlContext = new SQLContext(jsc);
			// 训练语料
			JavaRDD<LabeledPoint> training = jsc.objectFile(trainFile);
			// 持久化到内存，重复操作action不会重新从磁盘加载RDD
//			training.persist(StorageLevel.MEMORY_ONLY());

			JavaPairRDD<String,String> data = jsc.wholeTextFiles(urlTxtPath.getPath());
		    //要分类的文档RDD
		    JavaRDD<Tuple2<String,Vector>> test = convertDataRDD(jsc, sqlContext, data);
		    //开始文档分类
		    JavaPairRDD<String, Integer> prediction = doClassification(training, test);

			//从内存中移除
//			training.unpersist();

			UrlTxtPathDao.updateStatus(urlTxtPath.getId(), UrlTxtPathDao.STATUS_3);
		    try {
				List<Tuple2<String, Integer>> list = prediction.collect();
				// 更新至t_url_lib
				UrlLibUtil.updateUrlCategory(list);
				HdfsFileSystemUtil.delete(urlTxtPath.getPath());
				UrlTxtPathDao.updateStatus(urlTxtPath.getId(), UrlTxtPathDao.STATUS_4);
				logger.info("##########处理url的条数：" + list.size());
		    } catch (Exception e) {
				e.printStackTrace();
			}
			long end = new Date().getTime();
			logger.info("##########end（" + end + "）.............");
//			jsc.close();
			jsc.stop();
			
		}
	}

	/**
	 * 文档分类
	 * 
	 * @param training
	 *            训练语料RDD
	 * @param test
	 *            文档RDD
	 */
	private static JavaPairRDD<String, Integer> doClassification(JavaRDD<LabeledPoint> training,JavaRDD<Tuple2<String,Vector>> test){
	    //训练模型， Additive smoothing的值为1.0（默认值）
	    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
	    //开始匹配分类     urlMd5:predict
	    JavaPairRDD<String, Integer> prediction = test.mapToPair(new PairFunction<Tuple2<String,Vector>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<String,Vector> t) throws Exception {
				if (t._2==null || t._2.size()<=0) {
					return new Tuple2<String, Integer>(t._1, 0);
				}
				double predict = model.predict(t._2);
				//key=filename,value=predict
				return new Tuple2<String, Integer>(t._1, (int) predict);
			}
		});
	    return prediction;
	}

	static IkParticiple participle = new IkParticiple();
	private static JavaRDD<Tuple2<String,Vector>> convertDataRDD(JavaSparkContext jsc, SQLContext sqlContext, JavaPairRDD<String,String> data){
		
		//urlMd5:words
		JavaRDD<Row> jrdd = data.flatMap(new FlatMapFunction<Tuple2<String, String>, Row>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Row> call(Tuple2<String, String> v1) throws Exception {
				List<Row> list = new ArrayList<Row>();
				if (StringUtils.isEmpty(v1._2)) {
					return list;
				}
				String words[] = participle.participle(v1._2, true);
				String value = "";
				for (String w : words) {
					value += (w + " ");
				}
		    	String urlMd5 = v1._1.substring(v1._1.lastIndexOf("/")+1, v1._1.lastIndexOf("."));
				Row row = RowFactory.create(urlMd5, value);
				list.add(row);
				return list;
			}
		});

		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.StringType, false, Metadata.empty()),
						new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });
		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);

		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(MLConstants.NUM_FEATURES);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);

		//urlMd5:features
		JavaRDD<Tuple2<String, Vector>> trainDataRDD = rescaledData.select("features", "label").javaRDD()
				.map(new Function<Row, Tuple2<String, Vector>>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Vector> call(Row row) throws Exception {
						SparseVector features = row.getAs(0);
						// LabeledPoint代表一条训练数据，即打过标签的数据
						return new Tuple2<String, Vector>(row.getString(1), features);
					}

				});
		return trainDataRDD;
	}

	@Deprecated
	private static JavaRDD<Tuple2<String, Vector>> convertDataRDD(JavaSparkContext jsc, SQLContext sqlContext,
			List<UrlLib> urlLibList) {

		JavaRDD<UrlLib> distData = jsc.parallelize(urlLibList);
		
		// 要分类的文档RDD   urlMd5:text
		JavaPairRDD<String, String> data = distData.mapToPair(new PairFunction<UrlLib, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(UrlLib urlLib) throws Exception {
				String text = JsoupUtil.getText(urlLib.getUrl());
				//url失效时
				if (StringUtils.isEmpty(text)) {
					info("失效的网址url_md5：" + urlLib.getUrlMd5());
					invalidUrl.add(urlLib.getUrlMd5());
				}
				return new Tuple2<String, String>(urlLib.getUrlMd5(), text);
			}
		});
//		info("通过网址抓取网页内容条数：" + data.count());   //导致卡死
		
		//urlMd5:words
		JavaRDD<Row> jrdd = data.flatMap(new FlatMapFunction<Tuple2<String, String>, Row>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Row> call(Tuple2<String, String> v1) throws Exception {
				List<Row> list = new ArrayList<Row>();
				if (StringUtils.isEmpty(v1._2)) {
					return list;
				}
				String words[] = participle.participle(v1._2, true);
				String value = "";
				for (String w : words) {
					value += (w + " ");
				}
				Row row = RowFactory.create(v1._1, value);
				list.add(row);
				return list;
			}
		});

		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.StringType, false, Metadata.empty()),
						new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });
		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		DataFrame wordsData = tokenizer.transform(sentenceData);

		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(MLConstants.NUM_FEATURES);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);

		//urlMd5:features
		JavaRDD<Tuple2<String, Vector>> trainDataRDD = rescaledData.select("features", "label").javaRDD()
				.map(new Function<Row, Tuple2<String, Vector>>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Vector> call(Row row) throws Exception {
						SparseVector features = row.getAs(0);
						// LabeledPoint代表一条训练数据，即打过标签的数据
						return new Tuple2<String, Vector>(row.getString(1), features);
					}

				});
		return trainDataRDD;
	}
	
	/**
	 * 获取待分类的url
	 * 
	 * @return
	 */
	private static void info(String info) {
		logger.info(info);
	}
}

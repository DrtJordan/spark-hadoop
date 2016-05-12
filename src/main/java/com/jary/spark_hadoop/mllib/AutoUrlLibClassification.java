package com.jary.spark_hadoop.mllib;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
import org.apache.spark.storage.StorageLevel;

import com.jary.framework.core.util.DBHelper;
import com.jary.framework.core.util.JsoupUtil;
import com.jary.spark_hadoop.domain.UrlLib;

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

	private static final String trainFile = "/tmp/spark/20160427/train/";

	private static final int numFeatures = 50000;

	public static final int AD_CATEGORY_UNKNOWN_PUSH = 0;// 未知

	public static final int BATCH_NUM = 30;// 一次批量处理的url数

	public static final long TIME_INTERVAL = 1000 * 60 * 1;//时间间隔
	
	private static final Map<String,Integer> lineCountMap = new ConcurrentHashMap<String,Integer>();
	
	/**
	 * 失效的url
	 */
	private static final List<String> invalidUrl = new ArrayList<>();

	private static final String select_sql = "select url_md5, host, url, keyworld, ad_category, status from t_url_lib where status = "
			+ AD_CATEGORY_UNKNOWN_PUSH 
			+ " order by url_md5 ASC "
			+ " limit 0, " + BATCH_NUM + "";

	private static final String update_sql = "update t_url_lib set ad_category = ?, status = ? where url_md5 = ?";

	public static void main(String[] args) {
//		List<UrlLib> urlLibList = getUrlLibList();
//		Gson gson = new Gson();
//		System.out.println(gson.toJson(urlLibList));
//		
//		Connection conn = null;
//		try {
//			conn = DBHelper.getConnection();
//			conn.setAutoCommit(false);
//			PreparedStatement prest = conn.prepareStatement(update_sql, ResultSet.TYPE_SCROLL_SENSITIVE,
//					ResultSet.CONCUR_READ_ONLY);
//				prest.setInt(1, 123);
//				prest.setInt(2, 1);
//				prest.setString(3, urlLibList.get(0).getUrlMd5());
//				prest.execute();
//			conn.commit();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				conn.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
		
		
		SparkConf sparkConf = new SparkConf().setAppName("AutoUrlLibClassification");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		// 训练语料
		JavaRDD<LabeledPoint> training = jsc.objectFile(trainFile);
		// 持久化到内存，重复操作action不会重新从磁盘加载RDD
		training.persist(StorageLevel.MEMORY_ONLY());

		long start = new Date().getTime();
		logger.info("##########start（" + start + "）.............");
		// 1、从t_url_lib中获取待分类的url
		List<UrlLib> urlLibList = getUrlLibList();
		if (urlLibList != null && urlLibList.size() > 0) {
			logger.info("##########待处理url的条数：" + urlLibList.size());
			updateUrlStatus(urlLibList);
			// 2、检测url对应网页的类型
			// 要分类的文档RDD
			JavaRDD<Tuple2<String, Vector>> test = convertDataRDD(jsc, sqlContext, urlLibList);

			// 开始文档分类
			JavaPairRDD<String, Integer> matchCounts = doClassification(training, test);
		    try {
				List<Tuple2<String, Integer>> list = matchCounts.collect();
				
				Map<String,Integer[]> resultMap = new HashMap<String,Integer[]>();
			    for (Tuple2<String, Integer> tuple2 : list) {
			    	String fileName = tuple2._1.substring(0, tuple2._1.indexOf(","));
			    	String category = tuple2._1.substring(tuple2._1.indexOf(",")+1);
			    	
			    	Integer[] v = resultMap.get(fileName);
			    	if(v == null){
			    		v = new Integer[]{0,0};
			    		resultMap.put(fileName, v);
			    	}
			    	if(v[1] < tuple2._2){
			    		v[0] = (int)Double.parseDouble(category);
			    		v[1] = tuple2._2;
			    	}
				}
				// 3、更新至t_url_lib
				updateUrlCategory(resultMap);
				//4、处理失效的url
//				updateInvalidUrl(urlLibList);
				logger.info("##########失效url的条数：" + invalidUrl.size());
				logger.info("##########处理url的条数：" + resultMap.size());
		    } catch (Exception e) {
				e.printStackTrace();
			}    
		}
		long end = new Date().getTime();
		logger.info("##########end（" + end + "）.............");
//		jsc.close();
		jsc.stop();
	}

	/**
	 * 处理失效的url
	 * @param invalidUrl
	 */
	private static void updateInvalidUrl(List<String> invalidUrl) {
		if (invalidUrl!=null && invalidUrl.size()>0) {
			Connection conn = null;
			try {
				conn = DBHelper.getConnection();
				conn.setAutoCommit(false);
				PreparedStatement prest = conn.prepareStatement(update_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
				for (String url_md5 : invalidUrl) {
					logger.info("失效的网址url_md5：" + url_md5);
					prest.setInt(1, 0);
					prest.setInt(2, 2);
					prest.setString(3, url_md5);
					prest.addBatch();
				}
				prest.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 修改状态
	 * @param urlLibList
	 */
	private static void updateUrlStatus(List<UrlLib> urlLibList) {
		if (urlLibList!=null && urlLibList.size()>0) {
			Connection conn = null;
			try {
				conn = DBHelper.getConnection();
				conn.setAutoCommit(false);
				PreparedStatement prest = conn.prepareStatement(update_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
				for (UrlLib urlLib : urlLibList) {
					prest.setInt(1, 0);
					prest.setInt(2, 2);
					prest.setString(3, urlLib.getUrlMd5());
					prest.addBatch();
				}
				prest.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @param resultMap
	 */
	private static void updateUrlCategory(Map<String, Integer[]> resultMap) {
		if (resultMap!=null && resultMap.size()>0) {
			Connection conn = null;
			try {
				conn = DBHelper.getConnection();
				conn.setAutoCommit(false);
				PreparedStatement prest = conn.prepareStatement(update_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
				for (Map.Entry<String, Integer[]> entry : resultMap.entrySet()) {
					logger.info("网址url_md5：（" + entry.getKey() + "）的分类结果为：" + entry.getValue()[0]);
					prest.setInt(1, entry.getValue()[0]);
					prest.setInt(2, 1);
					prest.setString(3, entry.getKey());
					prest.addBatch();
				}
				prest.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 获取待分类的url
	 * 
	 * @return
	 */
	private static List<UrlLib> getUrlLibList() {
		List<UrlLib> urlLibList = new ArrayList<UrlLib>();
		ResultSet resultSet = DBHelper.executeQuery(select_sql);
		try {
			while (resultSet.next()) {
				UrlLib urlLib = new UrlLib();
				urlLib.setUrlMd5(resultSet.getString("url_md5"));
				urlLib.setUrl(resultSet.getString("url"));
//				urlLib.setKeyworld(resultSet.getString("keyworld"));
				urlLibList.add(urlLib);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return urlLibList;
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
	    //开始匹配分类
	    JavaRDD<String> prediction = test.map(new Function<Tuple2<String,Vector>, String>() {

			public String call(Tuple2<String,Vector> t) throws Exception {
				double predict = model.predict(t._2);
				//key=filename,value=predict
				return t._1+","+predict;
			}
		});
	    List<String> list = prediction.collect();
	    for (String filePath : list) {
	    	String fileName = filePath.substring(0, filePath.indexOf(","));
	    	Integer count = lineCountMap.get(fileName);
			if(count == null){
				count = 1;
			}else{
				count = count + 1;
			}
			lineCountMap.put(fileName,count);
		}
	    //计算匹配成功数
	    JavaPairRDD<String, Integer> matchCounts = prediction.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String t)
					throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
	    return matchCounts;
	}

	static IkParticiple participle = new IkParticiple();

	private static JavaRDD<Tuple2<String, Vector>> convertDataRDD(JavaSparkContext jsc, SQLContext sqlContext,
			List<UrlLib> urlLibList) {

		JavaRDD<UrlLib> distData = jsc.parallelize(urlLibList);
		
		// 要分类的文档RDD
		JavaPairRDD<String, String> data = distData.mapToPair(new PairFunction<UrlLib, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(UrlLib urlLib) throws Exception {
				String text = JsoupUtil.getText(urlLib.getUrl());
				//url失效时
				info("网址url_md5：" + urlLib.getUrlMd5() + "， 网页text:" + text);
				if (StringUtils.isEmpty(text)) {
					info("失效的网址url_md5：" + urlLib.getUrlMd5());
					invalidUrl.add(urlLib.getUrlMd5());
				}
				return new Tuple2<String, String>(urlLib.getUrlMd5(), text);
			}
		});
		info("通过网址抓取网页内容条数：" + data.count());
		JavaRDD<Row> jrdd = data.flatMap(new FlatMapFunction<Tuple2<String, String>, Row>() {
			private static final long serialVersionUID = 1L;
			// 检索字符，这个属性越短，匹配精度越高。
			private int charSize = 100;

			public Iterable<Row> call(Tuple2<String, String> v1) throws Exception {
				/*
				info("网址url_md5：" + v1._1 + "， 网页text:" + v1._2);
				if (StringUtils.isEmpty(v1._2)) {
					info("失效的网址url_md5：" + v1._1);
					invalidUrl.add(v1._1);
				}*/
				// 进行分隔
				List<String> lineList = new ArrayList<String>();
				int length = v1._2.length();
				int c = length / charSize;
				int index = 0;
				if (length > charSize) {
					for (int i = 0; i < c; i++) {
						index = charSize + i * charSize;
						lineList.add(v1._2.substring(i * charSize, charSize + i * charSize));
					}
				}
				if (index < length - 1) {
					lineList.add(v1._2.substring(index, length));
				}
				List<Row> list = new ArrayList<Row>();
				for (String line : lineList) {
					String words[] = participle.participle(line, true);
					String value = "";
					for (String w : words) {
						value += (w + " ");
					}
					Row row = RowFactory.create(v1._1, value);
					list.add(row);
				}
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
				.setNumFeatures(numFeatures);
		DataFrame featurizedData = hashingTF.transform(wordsData);
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		DataFrame rescaledData = idfModel.transform(featurizedData);

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

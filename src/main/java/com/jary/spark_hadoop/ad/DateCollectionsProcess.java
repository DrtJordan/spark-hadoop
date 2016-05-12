package com.jary.spark_hadoop.ad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jary.spark_hadoop.mllib.IkParticiple;

import cn.hadoop.spark_hadoop.domain.IpInfoResult;
import cn.hadoop.spark_hadoop.domain.UrlLib;
import cn.hadoop.spark_hadoop.util.DBHelper;
import cn.hadoop.spark_hadoop.util.DateUtil;
import cn.hadoop.spark_hadoop.util.JsoupUtil;
import cn.hadoop.spark_hadoop.util.MLConstants;
import cn.hadoop.spark_hadoop.util.StringUtil;
import cn.hadoop.spark_hadoop.util.TagUtil;
import cn.hadoop.spark_hadoop.util.UMConstants;
import cn.hadoop.spark_hadoop.util.UserAgentUtil;
import scala.Tuple2;
import tachyon.worker.DataServerMessage;

/**
 * 处理关键词日志，并打标签
 * @author hjl
 * @date 2016年6月14日 下午1:20:28 
 */
public class DateCollectionsProcess {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(DateCollectionsProcess.class);

	private static IkParticiple participle = new IkParticiple();

	private static final String SEPARATOR_NL = System.getProperty("line.separator");

//	private static final String logFile = "/Volumes/data/logs/webadserver/userdata.2016-04-06.log";
	
	
	public static void main(String[] args) {
		//  /tmp/spark/keyword/train/
		if(args.length < 3){
			System.err.println("Usage: TrainDataRecord <trainFile> <logFile> <date>");
			System.exit(1);
		}
		String trainFile = args[0];  //TODO /tmp/spark/keyword/train/  TODO 从数据库取，生产training
		String logFile = args[1];// "/Volumes/data/logs/webadserver/userdata";
		String date = args[2];// "2016-04-06";

		SparkConf sparkConf = new SparkConf().setAppName("DateCollectionsProcess");//.setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		// 训练语料
		JavaRDD<LabeledPoint> training = jsc.objectFile(trainFile);
		// 持久化到内存，重复操作action不会重新从磁盘加载RDD
		training.persist(StorageLevel.MEMORY_ONLY());

		JavaRDD<String> log = jsc.textFile(logFile + "." + date + ".log");
		//打标签
		JavaRDD<String> log1 = log.map(new Function<String, String>() {

			@Override
			public String call(String info) throws Exception {
				info = info.substring(info.indexOf("INFO  -") + 8);
				return info;
			}
			
		});
		// 要分类的文档RDD
		JavaRDD<Tuple2<String, Vector>> test = convertDataRDD(jsc, sqlContext, log1);
		// 开始文档分类
		JavaRDD<List<Object>> result = doClassification(training, test);
		List<List<Object>> recordList = result.collect();
		//
		//批量打标签list
		DBHelper.executeNonQuery(TagUtil.SQL_MARK_INSERT, recordList);
		//批量打标签list
		jsc.close();
	}

	/**
	 * 文档分类
	 * 
	 * @param training
	 *            训练语料RDD
	 * @param test
	 *            文档RDD
	 */
	private static JavaRDD<List<Object>> doClassification(JavaRDD<LabeledPoint> training,JavaRDD<Tuple2<String,Vector>> test){
	    //训练模型， Additive smoothing的值为1.0（默认值）
	    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
	    //开始匹配分类
	    JavaRDD<List<Object>> result = test.map(new Function<Tuple2<String,Vector>, List<Object>>() {

			public List<Object> call(Tuple2<String,Vector> t) throws Exception {
				double predict = model.predict(t._2);
				List<Object> tag = new ArrayList<Object>();
				tag.add((int)predict);//tagId
				tag.add(t._1);//sid
				tag.add(2);//rela_type
				tag.add(1);//weight
				tag.add(DateUtil.getCurrentDate());//update_time
				tag.add(DateUtil.getCurrentDate());//create_time
				tag.add(1);//status
				return tag;
			}
		});
	    return result;
	}

	/**
	 * @param jsc
	 * @param sqlContext
	 * @param log
	 * @return
	 */
	private static JavaRDD<Tuple2<String, Vector>> convertDataRDD(JavaSparkContext jsc, SQLContext sqlContext,
			JavaRDD<String> log) {

		// 要分类的文档RDD
		JavaPairRDD<String, String> data = log.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String info) throws Exception {
				String[] datas = info.split("&");
				return new Tuple2<String, String>(datas[0].split("=")[1], datas[3]);
			}
		});
		JavaRDD<Row> jrdd = data.flatMap(new FlatMapFunction<Tuple2<String, String>, Row>() {
			private static final long serialVersionUID = 1L;
			// 检索字符，这个属性越短，匹配精度越高。
			private int charSize = 10;

			public Iterable<Row> call(Tuple2<String, String> v1) throws Exception {
				
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
				.setNumFeatures(MLConstants.NUM_FEATURES);
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
}

package com.jary.spark_hadoop.mllib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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

import cn.hadoop.spark_hadoop.domain.Category;
import cn.hadoop.spark_hadoop.util.CategoryUtil;
import cn.hadoop.spark_hadoop.util.MLConstants;
import cn.hadoop.spark_hadoop.util.StringUtil;
import scala.Tuple2;
/**
 * 批量文章分类
 * @author Jiandoang
 *
 */
public class BatchTextClassification {


	private static Logger logger = Logger.getLogger(BatchTextClassification.class);

	private static final String SEPARATOR_NL = System.getProperty("line.separator");
	
	private static final Map<String,Integer> lineCountMap = new ConcurrentHashMap<String,Integer>();
	
	private static Map<Integer, String> categoryMap = new ConcurrentHashMap<Integer, String>();
	
	static {
		List<Category> list = CategoryUtil.getCategoryList();
		if (list!=null && list.size()>0) {
			for (Category category : list) {
				categoryMap.put(category.getId(), category.getTypeName());
			}
		}
	}
	
	public static void main(String[] args) {
		if(args.length < 3){
			System.err.println("Usage: SougouTextClassification <trainFile> <filePath> <outPath>");
			System.exit(1);
		}
		String trainFile = args[0];
		String filePath = args[1];
		String outPath = args[2];
		File file = new File(outPath);
		if( file.exists() ){
			logger.error("output file already exists:"+outPath);
			return;
		}else{
			try {
				file.createNewFile();
			} catch (IOException e) {}
		}
		
	    SparkConf sparkConf = new SparkConf().setAppName("BatchTextClassification");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new SQLContext(jsc);
	    //训练语料
	    JavaRDD<LabeledPoint> training = jsc.objectFile(trainFile);
	    //持久化到内存，重复操作action不会重新从磁盘加载RDD
	    training.persist(StorageLevel.MEMORY_ONLY());
//	    Map<LabeledPoint, Long> map = training.countByValue();
//	    ArrayList<Entry<LabeledPoint, Long>> trainList = new ArrayList<>(map.entrySet());
//	    Collections.sort(trainList, new Comparator<Entry<LabeledPoint, Long>>() {
//			@Override
//			public int compare(Entry<LabeledPoint, Long> o1, Entry<LabeledPoint, Long> o2) {
//				if (o1.getValue()>o2.getValue()) {
//					return -1;
//				} else if (o1.getValue()>o2.getValue()) {
//					return 1;
//				}
//				return 0;
//			}
//		});
	    
	    //要分类的文档RDD
	    JavaRDD<Tuple2<String,Vector>> test = convertDataRDD(jsc, sqlContext, filePath);
	    //开始文档分类
	    JavaPairRDD<String, Integer> matchCounts = doClassification(training, test);
	    try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
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
		    double correct = 0.0;
		    //根据类型排序，然后输出
		    List<String> keyList = new ArrayList<String>();
		    Map<Integer, Integer> disturb = new HashMap<Integer, Integer>();
		    Map<Integer, Integer> correctMap = new HashMap<Integer, Integer>();
		    Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();
		    keyList.addAll(resultMap.keySet());
		    Collections.sort(keyList);
		    for (String key : keyList) {
		    	Integer count = lineCountMap.get(key);
		    	String[] s = key.split("/");
		    	Integer num = Integer.parseInt(s[s.length - 2].substring(1));
				Integer[] value = resultMap.get(key);
		    	if(num.equals(value[0])){//命中次数
		    		correct++;
		    		Integer countTmp2 = correctMap.get(num);
		    		if (countTmp2==null) {
		    			countTmp2 = 0;
					}
		    		countTmp2++;
		    		correctMap.put(num, countTmp2);
		    	} else {//统计干扰次数
		    		Integer countTmp = disturb.get(value[0]);
		    		if (countTmp==null) {
		    			countTmp = 0;
					}
		    		countTmp++;
		    		disturb.put(value[0], countTmp);
				}
		    	Integer cateNum = countMap.get(num);
	    		if (cateNum==null) {
	    			cateNum = 0;
				}
	    		cateNum++;
	    		countMap.put(num, cateNum);
		    	writer.write(key + " cate:" + value[0] + " hits:" + value[1] + " count:" + count + "  hitRate:"+ ((value[1]+0.0) / count) + SEPARATOR_NL);
			}
		    
//		    for (Map.Entry<String, Integer[]> entry : resultMap.entrySet()) {
//		    	Integer count = lineCountMap.get(entry.getKey());
//		    	String[] s = entry.getKey().split("/");
//				double num = Double.parseDouble(s[s.length - 2].substring(1));
//		    	if(entry.getValue()[0] == num){
//		    		correct++;
//		    	}
//		    	writer.write(entry.getKey() + " cate:" + entry.getValue()[0] + " hits:" + entry.getValue()[1] + " count:" + count + "  hitRate:"+ ((entry.getValue()[1]+0.0) / count) + SEPARATOR_NL);
//			}
		    writer.write("匹配数："+correct+" 总数："+resultMap.size()+" 准确率："+(correct * 100.0 / resultMap.size()) + "%" + SEPARATOR_NL);
		    
		    //将Map转化为List集合，List采用ArrayList  
//	        List<Map.Entry<Integer, Integer>> list_Data = new ArrayList<Map.Entry<Integer, Integer>>(disturb.entrySet());  
//	        for (Entry<Integer, Integer> entry : list_Data) {
//		    	writer.write("cate:" + entry.getKey() + " count:" + entry.getValue() + SEPARATOR_NL);
//			}

	        List<Map.Entry<Integer, Integer>> list_Data = new ArrayList<Map.Entry<Integer, Integer>>(correctMap.entrySet());  
	        //通过Collections.sort(List I,Comparator c)方法进行排序  
	        Collections.sort(list_Data, new Comparator<Map.Entry<Integer, Integer>>() {
	            @Override  
	            public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {  
	                return (o2.getValue() - o1.getValue());  
	            }  
	        });
	        for (Entry<Integer, Integer> entry : list_Data) {
		    	writer.write("分类名：" + StringUtil.append2length(categoryMap.get(entry.getKey()), 6, "  ")  + " 分类号:" + StringUtil.append2length(entry.getKey()+"", 10, " ") + "正确个数:" + entry.getValue() + "    干扰个数:" + ( disturb.get(entry.getKey())==null? 0:disturb.get(entry.getKey()) ) + "   准确率：" + (entry.getValue() * 100.0 / countMap.get(entry.getKey())) + "%" + SEPARATOR_NL);
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 文档分类
	 * @param training 训练语料RDD
	 * @param test 文档RDD
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
	static Participle participle = new IkParticiple();
	private static JavaRDD<Tuple2<String,Vector>> convertDataRDD(JavaSparkContext jsc,SQLContext sqlContext,String filePath){
		JavaPairRDD<String,String> data = jsc.wholeTextFiles(filePath);
		JavaRDD<Row> jrdd = data.flatMap(new FlatMapFunction<Tuple2<String,String>, Row>() {

			//检索字符，这个属性越短，匹配精度越高。
			private int charSize = 100;
			public Iterable<Row> call(Tuple2<String, String> v1)
					throws Exception {
				//对每行进行分隔
				List<String> lineList = new ArrayList<String>();
				int length = v1._2.length();
				int c = length / charSize;
				int index = 0;
				if( length > charSize ){
					for (int i = 0; i < c; i++) {
						index = charSize+i * charSize;
						lineList.add(v1._2.substring(i * charSize,charSize+i * charSize));
					}
				}
				if( index < length - 1){
					lineList.add(v1._2.substring(index,length));
				}
				List<Row> list = new ArrayList<Row>();
				for (String line : lineList) {
					String words[] = participle.participle(line, true);
					String value = "";
					for (String w : words) {
						value+=(w+" ");
					}
			        Row row = RowFactory.create(v1._1,value);
					list.add(row);
				}
				return list;
			}
		});
		
		StructType schema = new StructType(new StructField[] {
				new StructField("label", DataTypes.StringType, false,
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
		
		JavaRDD<Tuple2<String,Vector>> trainDataRDD = rescaledData.select("features", "label").javaRDD().map(new Function<Row, Tuple2<String,Vector>>() {

			public Tuple2<String,Vector> call(Row row) throws Exception {
				SparseVector features = row.getAs(0);
		        //LabeledPoint代表一条训练数据，即打过标签的数据
		        return new Tuple2<String,Vector>(row.getString(1), features);
			}
	    	
		});
		return trainDataRDD;
	}
}

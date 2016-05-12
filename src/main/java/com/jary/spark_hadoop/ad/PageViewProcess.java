package com.jary.spark_hadoop.ad;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.hadoop.spark_hadoop.domain.IpInfoResult;
import cn.hadoop.spark_hadoop.util.DBHelper;
import cn.hadoop.spark_hadoop.util.DateUtil;
import cn.hadoop.spark_hadoop.util.JsoupUtil;
import cn.hadoop.spark_hadoop.util.StringUtil;
import cn.hadoop.spark_hadoop.util.TagUtil;
import cn.hadoop.spark_hadoop.util.UMConstants;
import cn.hadoop.spark_hadoop.util.UserAgentUtil;

/**
 * 处理pv日志 TODO
 * 用来统计用户访问行为
 * @author hjl
 * @date 2016年6月14日 下午1:20:28 
 */
public class PageViewProcess {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(PageViewProcess.class);

	private static final String SEPARATOR_NL = System.getProperty("line.separator");

//	private static final String logFile = "/Volumes/data/logs/adsdkserver/page_view.2016-06-06.log";
	
	
	public static void main(String[] args) {
		if(args.length < 2){
			System.err.println("Usage: TrainDataRecord <logFile> <date>");
			System.exit(1);
		}
		String logFile = args[0];
		String date = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("PageViewProcess");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<String> log = jsc.textFile(logFile + "." + date + ".log");
		
		
		//打标签
		JavaRDD<String> log1 = log.map(new Function<String, String>() {

			@Override
			public String call(String info) throws Exception {
				info = info.substring(info.indexOf("INFO  -") + 8);
				JSONObject object = JSON.parseObject(info);
				object.getString(UMConstants.IP);
				object.getString(UMConstants.CHANNEL);
				object.getString(UMConstants.AP_UID);
				object.getString(UMConstants.SESSION_ID);
				object.getString(UMConstants.USER_TYPE);
				object.getString(UMConstants.HOST);
				object.getString(UMConstants.CAT_ID);
				return info;
			}
			
		});
		jsc.close();
	}
}

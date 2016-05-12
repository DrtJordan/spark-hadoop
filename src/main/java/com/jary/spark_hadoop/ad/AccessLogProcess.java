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
import com.jary.spark_hadoop.domain.IpInfoResult;
import com.jary.spark_hadoop.util.DBHelper;
import com.jary.spark_hadoop.util.DateUtil;
import com.jary.spark_hadoop.util.JsoupUtil;
import com.jary.spark_hadoop.util.StringUtil;
import com.jary.spark_hadoop.util.TagUtil;
import com.jary.spark_hadoop.util.UserAgentUtil;

/**
 * 处理访问日志
 * @author hjl
 * @date 2016年6月14日 下午1:20:28 
 */
public class AccessLogProcess {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(AccessLogProcess.class);

	private static final String SEPARATOR_NL = System.getProperty("line.separator");

//	private static final String logFile = "/Volumes/data/logs/webadserver/advisitor.2016-06-06.log";
	
	
	public static void main(String[] args) {
		if(args.length < 2){
			System.err.println("Usage: TrainDataRecord <logFile> <date>");
			System.exit(1);
		}
		String logFile = args[0];
		String date = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("AdVisitorLogProcess");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<String> log = jsc.textFile(logFile + "." + date + ".log");
		
		//筛选有效的日志
		JavaRDD<String> log1 = log.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				if (line.indexOf("INFO  -")>-1) {
					return true;
				}
				return false;
			}
			
		});
		
		//打标签
		JavaRDD<String> log2 = log1.map(new Function<String, String>() {

			@Override
			public String call(String info) throws Exception {
				info = info.substring(info.indexOf("INFO  -") + 8);
				String[] datas = info.split("||");
				
				List<List<Object>> recordList = new ArrayList<>();
				
				//系统类型
				List<Object> osTag = new ArrayList<Object>();
				osTag.add(TagUtil.getTagIdByOsType(Integer.valueOf(datas[5])));//tagId
				osTag.add(datas[1]);//sid
				osTag.add(2);//rela_type
				osTag.add(1);//weight
				osTag.add(DateUtil.getCurrentDate());//update_time
				osTag.add(DateUtil.getCurrentDate());//create_time
				osTag.add(1);//status
				recordList.add(osTag);
				
				//浏览器
				List<Object> browserTag = new ArrayList<Object>();
				browserTag.add(UserAgentUtil.getBrowserName(datas[3]));//tagId
				browserTag.add(datas[1]);//sid
				browserTag.add(2);//rela_type
				browserTag.add(1);//weight
				browserTag.add(DateUtil.getCurrentDate());//update_time
				browserTag.add(DateUtil.getCurrentDate());//create_time
				browserTag.add(1);//status
				recordList.add(browserTag);
				
				//屏幕分辨率
				
				//地理位置（省）
				String result = JsoupUtil.getContent(JsoupUtil.IP_API_TAOBAO+datas[0]);
		  		IpInfoResult ipInfoResult = (IpInfoResult) JSON.parseObject(result, IpInfoResult.class);
				if (ipInfoResult!=null && "0".equals(ipInfoResult.getCode())) {
					//省份
					String province = ipInfoResult.getData().getRegion();
					province = StringUtil.removeAreaKeyWord(province);
					Integer tagId = TagUtil.proviceMap.get(province);
					List<Object> proviceTag = new ArrayList<Object>();
					proviceTag.add(tagId);//tagId
					proviceTag.add(datas[1]);//sid
					proviceTag.add(2);//rela_type
					proviceTag.add(1);//weight
					proviceTag.add(DateUtil.getCurrentDate());//update_time
					proviceTag.add(DateUtil.getCurrentDate());//create_time
					proviceTag.add(1);//status
					proviceTag.add(browserTag);
				} else {
					logger.info("IP[%s]获取IP信息失败！", datas[0]);
				}
				
				DBHelper.executeNonQuery(TagUtil.SQL_MARK_INSERT, recordList);
				return info;
			}
			
		});
		jsc.close();
	}
}

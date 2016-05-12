package com.jary.spark_hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class IPAddressStat implements Serializable{
	
	private static final long serialVersionUID = -3300950052243759739L;
	private static Logger logger = Logger.getLogger(IPAddressStat.class);
	private static final String SEPARATOR_NL = System.getProperty("line.separator");
	public static class IPInfo {
		public static String apiUrl = "http://api.hostip.info/get_html.php?position=true&ip=";
		private String country;
		private String city;
		private String latitude;
		private String longitude;
		private String ip;
		
		public IPInfo(String ipAddress){
			try{
				String result = doGet(apiUrl + ipAddress);
				if(StringUtils.isNotEmpty(result)){
					String split[] = result.split(SEPARATOR_NL);
					country = split[0].split(":")[1].trim();
					city = split[1].split(":")[1].trim();
					latitude = split[3].split(":")[1].trim();
					longitude = split[4].split(":")[1].trim();
					ip = split[5].split(":")[1].trim();
				}
			}catch(Exception err){
				logger.error("Get ip["+ipAddress+"] info error:"+err.getMessage());
			}
		}
		@SuppressWarnings("finally")
		private String doGet(String url) throws Exception {
			BufferedReader in = null;
			String content = null;
			try {
				// 定义HttpClient
				HttpClient client = new DefaultHttpClient();
				// 实例化HTTP方法
				HttpGet request = new HttpGet();
				request.setURI(new URI(url));
				HttpResponse response = client.execute(request);

				in = new BufferedReader(new InputStreamReader(response.getEntity()
						.getContent()));
				StringBuffer sb = new StringBuffer("");
				String line = "";
				while ((line = in.readLine()) != null) {
					sb.append(line + SEPARATOR_NL);
				}
				in.close();
				content = sb.toString();
			} finally {
				if (in != null) {
					try {
						in.close();// 最后要关闭BufferedReader
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				return content;
			}
		}
		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}
 
		public String getLatitude() {
			return latitude;
		}
		public void setLatitude(String latitude) {
			this.latitude = latitude;
		}
		public String getLongitude() {
			return longitude;
		}
		public void setLongitude(String longitude) {
			this.longitude = longitude;
		}
		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

	}

	/**
	 * 开始统计用户IP
	 */
	@SuppressWarnings("serial")
	public void start(String inPath,String outPath){
		SparkConf sparkConf = new SparkConf().setAppName("IPAddressStat");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inPath, 1);
		//读取每行数据，提取ip列表
		JavaRDD<String> ipList = lines.flatMap(new FlatMapFunction<String, String>() {
			
			private List<String> empty = new ArrayList<String>(0);
			public Iterable<String> call(String line) throws Exception {
				String data[] = line.split("\\|\\|");
				if(data.length > 2){
					String []s = data[0].split(" ");
					List<String> list = new ArrayList<String>(1);
					list.add(s[s.length-1]);
					return list;
				}else{
					return empty;
				}
			}
		});
		//
		JavaPairRDD<String,Integer> ones = ipList.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String ip) throws Exception {
				return new Tuple2<String, Integer>(ip,  1);
			}
		});
		//reduce IP访问频率汇总
		JavaPairRDD<String,Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		//返回计算结果
		List<Tuple2<String, Integer>> output = counts.sortByKey().collect();
		//输出计算结果
		//counts.saveAsTextFile(outPath);
		wirteResult(new ArrayList<Tuple2<String,Integer>>(output), outPath);
		//spark程序停止
		ctx.stop();
		//关闭上下文资源
		ctx.close();
	}
	
	private void wirteResult(List<Tuple2<String, Integer>> output,
			String outPath) {
		
		try {
			File file = new File(outPath);
			if( file.exists() ){
				logger.error("output file already exists:"+outPath);
				return;
			}else{
				file.createNewFile();
			}
			// sort statistics result by value
			Collections.sort(output, new Comparator<Tuple2<String, Integer>>() {
			   public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
			        if(t1._2 < t2._2) {
			             return 1;
			        } else if(t1._2 > t2._2) {
			             return -1;
			        }
			        return 0;
			   }
			});
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
			for (Tuple2<String, Integer> tuple2 : output) {
				/*IPInfo info = new IPInfo(tuple2._1);
				writer.write(String.format("[%s],[%s],%s-%s,%s,%d"+SEPARATOR_NL, info.getCountry(),info.getCity(),info.getLatitude(),info.getLongitude(),tuple2._1,tuple2._2));*/
				writer.write(String.format("%s,%d"+SEPARATOR_NL, tuple2._1,tuple2._2));
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public static void main(String[] args) throws Exception {
		/*args = new String[]{ "hdfs://183.232.39.28:8020/userlog/access/ad_req.2015-11-27.log"
							,"hdfs://183.232.39.28:8020/userlog/ip_count_rs"
		};*/
		if(args.length < 2){
			System.err.println("Usage: IPAddressStat <inFile> <outFile>");
			System.exit(1);
		}
		String inPath = args[0];
		String outPath = args[1];
		IPAddressStat ipAddressStat = new IPAddressStat();
		ipAddressStat.start(inPath, outPath);
	}
}

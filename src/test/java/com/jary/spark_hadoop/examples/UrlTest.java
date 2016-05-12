package com.jary.spark_hadoop.examples;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.hadoop.spark_hadoop.domain.UrlLib;
import cn.hadoop.spark_hadoop.util.DBHelper;
import cn.hadoop.spark_hadoop.util.JsoupUtil;
import cn.hadoop.spark_hadoop.util.Md5Util;
import cn.hadoop.spark_hadoop.util.StringUtil;
import cn.hadoop.spark_hadoop.util.TxtUtil;

/**
 * @author ll
 * @date 2016年5月6日 上午9:55:53 
 */
public class UrlTest {

	private static final String insert_sql = "INSERT INTO t_url_lib (url_md5, host, url, keyworld, category, status) VALUES (?, ?, ?, ?, ?, ?);";

	private static final String count_sql = "SELECT url_md5 FROM t_url_lib WHERE url_md5=?;";

//	public static void crawlUrl() {
//		List<UrlLib> urlLibList = new ArrayList<>();
//		for (int i=1; i<101; i++) {
//			try {
//				org.jsoup.Connection connection = Jsoup.connect("http://finance.qq.com/c/gdyw_"+i+".htm?0.7452573564405784");
//				connection.timeout(100000);
//				Document document = connection.get();
//				Element div = document.getElementById("listZone");
//				Elements list = div.getElementsByClass("Q-tpWrap");
//				for (Element element : list) {
//					Elements links = element.getElementsByTag("a");
//					Element link = links.get(0);
//					String linkHref = link.attr("href");
//					String a_url = geturl(linkHref, url[0]);
//					if (a_url.startsWith("http://finance.sina.com")) {
//		                String linkText = link.text().trim();
//		                UrlLib urlLib = new UrlLib();
//		                urlLib.setUrl(a_url);
//		                urlLib.setHost(StringUtil.getHost(urlLib.getUrl()));
//		                urlLib.setUrlMd5(Md5Util.MD5(linkHref));
//		                urlLib.setKeyworld(linkText);
//		                urlLib.setCategory(8);
//		                urlLib.setStatus(0);
//		                urlLibList.add(urlLib);
//					}
//				}
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		saveUrl(urlLibList);
//	}

	public static void crawlUrlFromTxt(String filePath) {

		List<UrlLib> urlLibList = new ArrayList<>();
		try {
			//1、读取txt 每一行作为一个url
			File file = new File(filePath);
			List<String> finance_url = TxtUtil.readTxtByLine(file);
			//2、转化为Object
			for (String url : finance_url) {
                UrlLib urlLib = new UrlLib();
                urlLib.setUrl(url);
                urlLib.setHost(StringUtil.getHost(urlLib.getUrl()));
                urlLib.setUrlMd5(Md5Util.MD5(url));
                urlLib.setKeyworld("");
                urlLib.setCategory(8);
                urlLib.setStatus(0);
				if (!DBHelper.isExist(count_sql, urlLib.getUrlMd5())) {
	                urlLibList.add(urlLib);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("整理出url数目：" + urlLibList.size());
		batchSaveUrl(urlLibList);
	}

	public static List<UrlLib> convert(Set<String> urlList) {

		List<UrlLib> urlLibList = new ArrayList<>();
		try {
			//转化为Object
	    	for(String url : urlList){
				if (url==null || "".equals(url) || DBHelper.isExist(count_sql, Md5Util.MD5(url))) {
					continue;
				}
                UrlLib urlLib = new UrlLib();
                urlLib.setUrl(url);
                urlLib.setHost(StringUtil.getHost(url));
                urlLib.setUrlMd5(Md5Util.MD5(url));
                urlLib.setKeyworld("");
                urlLib.setCategory(8);
                urlLib.setStatus(0);
                urlLibList.add(urlLib);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return urlLibList;
	}

	public static List<String> crawlUrlByJsoup(String indexUrl) {

		List<String> urlList = new ArrayList<>();
		try {
			//1、抓取url
	    	Document doc = JsoupUtil.getDocument(indexUrl);
	    	Elements hrefs = doc.select("a[href]");
			//2、转化为Object
	    	for(Element elem:hrefs){
	    		String url = elem.attr("abs:href");
				if (url==null || "".equals(url)) {
					continue;
				}
                urlList.add(url);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return urlList;
	}

	public static void saveUrl(List<UrlLib> urlLibList) {
		if (urlLibList==null || urlLibList.size()<1) {
			return;
		}
		for (UrlLib urlLib : urlLibList) {
			try {
				if (!DBHelper.isExist(count_sql, urlLib.getUrlMd5())) {
					DBHelper.executeNonQuery(insert_sql, urlLib.getUrlMd5(), urlLib.getHost(), urlLib.getUrl(), urlLib.getKeyworld(), urlLib.getCategory(), urlLib.getStatus());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void batchSaveUrl(List<UrlLib> urlLibList) {
		if (urlLibList==null || urlLibList.size()<1) {
			return;
		}
		try {
			java.sql.Connection conn = DBHelper.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement prest = conn.prepareStatement(insert_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			for (UrlLib urlLib : urlLibList) {
				prest.setString(1, urlLib.getUrlMd5());
				prest.setString(2, urlLib.getHost());
				prest.setString(3, urlLib.getUrl());
				prest.setString(4, urlLib.getKeyworld());
				prest.setInt(5, urlLib.getCategory());
				prest.setInt(6, urlLib.getStatus());
				prest.addBatch();
			}
			prest.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param linkHref
	 * @param string
	 * @return
	 */
	public static String geturl(String linkHref, String url) {
		if (linkHref.startsWith("/")) {
			linkHref = "http://" + StringUtil.getHost(url) + linkHref;
		}
		return linkHref;
	}

    
    public static void main(String[] args) {
    	// http://finance.qq.com/a/20160506/030587.htm
//    	List<String> containList = new ArrayList<>();
//    	containList.add("doc-");
//    	containList.add("finance.sina.com.cn/");
//    	containList.add(".shtml");
//    	Set<String> urlSet = JsoupUtil.crawlUrlByIndex("http://finance.sina.com.cn/", containList);
//    	List<UrlLib> urlLibList = convert(urlSet);
//    	batchSaveUrl(urlLibList);
    	
    	String[] txts = {
//    			"D:/test/2016070514.txt",
//    			"D:/test/2016070610.txt",
//    			"D:/test/2016070615.txt",
//    			"D:/test/2016070714.txt",
//    			"D:/test/2016070810.txt",
//    			"D:/test/2016070819.txt",
//    			"D:/test/2016071111.txt",
    			"D:/test/2016071210.txt"
    	};
    	for (int i = 0; i < txts.length; i++) {
        	crawlUrlFromTxt(txts[i]);
		}
    	
	}
}

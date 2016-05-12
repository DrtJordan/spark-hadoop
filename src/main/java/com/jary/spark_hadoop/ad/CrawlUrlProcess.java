package com.jary.spark_hadoop.ad;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.jary.spark_hadoop.domain.UrlLib;
import com.jary.spark_hadoop.util.DBHelper;
import com.jary.spark_hadoop.util.JsoupUtil;
import com.jary.spark_hadoop.util.Md5Util;
import com.jary.spark_hadoop.util.StringUtil;

/**
 * 抓取url，并保存至t_url_lib
 * 新浪-财经
 * @author hjl
 * @date 2016年7月11日 下午6:25:55 
 */
public class CrawlUrlProcess {


	private static final String insert_sql = "INSERT INTO t_url_lib (url_md5, host, url, keyworld, category, status) VALUES (?, ?, ?, ?, ?, ?);";

	private static final String count_sql = "SELECT url_md5 FROM t_url_lib WHERE url_md5=?;";

	public static List<UrlLib> convert(Set<String> urlList) {

		List<UrlLib> urlLibList = new ArrayList<>();
		try {
			//转化为Object
	    	for(String url : urlList){
				if (StringUtils.isEmpty(url) || DBHelper.isExist(count_sql, Md5Util.MD5(url))) {
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
	
	public static void main(String[] args) {

    	List<String> containList = new ArrayList<>();
    	containList.add("doc-");
    	containList.add("finance.sina.com.cn/");
    	containList.add(".shtml");
    	Set<String> urlSet = JsoupUtil.crawlUrlByIndex("http://finance.sina.com.cn/", containList);
    	List<UrlLib> urlLibList = convert(urlSet);
    	batchSaveUrl(urlLibList);
	}
}

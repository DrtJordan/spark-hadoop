package com.jary.spark_hadoop.util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.hadoop.spark_hadoop.domain.UrlLib;
import scala.Tuple2;

/**
 * @author hjl
 * @date 2016年8月24日 上午10:38:19 
 */
public class UrlLibUtil {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(UrlLibUtil.class);

	public static final int AD_CATEGORY_UNKNOWN_PUSH = 0;// 类型未知

	public static final int CATEGORY_DEFAULT = 10000;// 普通投放

	public static final int BATCH_NUM = 400;// 一次批量处理的url数

	public static final int THREAD_NUM = 6;// 线程数

	public static final String urlTxtPath = "/tmp/spark/urltxt/";

	public static final String count_sql = "select count(1) from t_url_lib where status =? ";

	public static final String select_sql = "select url_md5, host, url, keyworld, ad_category, status from t_url_lib where status = "
			+ AD_CATEGORY_UNKNOWN_PUSH 
			+ " order by create_time DESC, url_md5 ASC "
			+ " limit 0, " + BATCH_NUM + " ";

	public static final String update_status_sql = "update t_url_lib set ad_category = ?, status = ? where url_md5 = ? ";


	/**
	 * 获取待分类的数量
	 * 
	 * @return
	 */
	public static int countByStatus(int status) {
		int count = 0;
		try {
			ResultSet resultSet = DBHelper.executeQuery(count_sql, status);
			if (resultSet.next()) {
				count = resultSet.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}


	/**
	 * 获取待分类的url
	 * 
	 * @return
	 */
	public static List<UrlLib> getUrlLibList() {
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
	 * 修改状态
	 * @param urlLibList
	 */
	public static void updateUrlStatus(List<UrlLib> urlLibList) {
		if (urlLibList!=null && urlLibList.size()>0) {
			Connection conn = null;
			try {
				conn = DBHelper.getConnection();
				conn.setAutoCommit(false);
				PreparedStatement prest = conn.prepareStatement(update_status_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
				for (UrlLib urlLib : urlLibList) {
					prest.setInt(1, CATEGORY_DEFAULT);
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
	 * @param list
	 */
	public static void updateUrlCategory(List<Tuple2<String, Integer>> list) {

		if (list!=null && list.size()>0) {
			Connection conn = null;
			try {
				conn = DBHelper.getConnection();
				conn.setAutoCommit(false);
				PreparedStatement prest = conn.prepareStatement(update_status_sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
				for (Tuple2<String, Integer> tuple2 : list) {
					int status = 1;
					logger.info("网址url_md5：（" + tuple2._1 + "）的分类结果为：" + tuple2._2);
					prest.setInt(1, tuple2._2);
					if (tuple2._2 == 0) {
						status = 2;
					}
					prest.setInt(2, status);
					prest.setString(3, tuple2._1);
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
	
	public static void saveUrlTxt(List<UrlLib> urlLibList, String path) {
		File pathD = new File(path);
		if (!pathD.exists()) {
			pathD.mkdirs();
		}
		for (UrlLib urlLib : urlLibList) {
			try {
				String text = JsoupUtil.getText(urlLib.getUrl());
				if (text==null || "".equals(text)) {
					continue;
				}
				File file = new File(path + urlLib.getUrlMd5() + ".txt");
				TxtUtil.writerTxt(file , text);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}

	/**
	 * @return
	 */
	public static String getTxtPath(Long timeMillis) {
		return UrlLibUtil.urlTxtPath + timeMillis + "/";
	}
	
	public static void main(String[] args) {
		countByStatus(0);
	}
}

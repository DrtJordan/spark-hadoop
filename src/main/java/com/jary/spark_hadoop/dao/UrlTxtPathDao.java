package com.jary.spark_hadoop.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.hadoop.spark_hadoop.domain.UrlTxtPath;
import cn.hadoop.spark_hadoop.util.DBHelper;
import cn.hadoop.spark_hadoop.util.DateUtil;

/**
 * @author hjl
 * @date 2016年8月31日 上午11:33:07 
 */
public class UrlTxtPathDao {
	
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(UrlTxtPathDao.class);
	
	private static final String insert_sql = "INSERT INTO `t_url_txt_path` (`id`, `path`, `create_time`, `update_time`, `status`) VALUES (?, ?, ?, ?, ?);";
	
	private static final String updatre_status_sql = "UPDATE `t_url_txt_path` SET `status`=?, `update_time`=? WHERE (`id`=?);";

	private static final String select_sql = "select `id`, `path`, `create_time`, `update_time`, `status` from `t_url_txt_path` ";

	//状态: 0：初始化；1：待处理；2：处理中；3：待删除；4：完成
	public static final Integer STATUS_0 = 0;
	public static final Integer STATUS_1 = 1;
	public static final Integer STATUS_2 = 2;
	public static final Integer STATUS_3 = 3;
	public static final Integer STATUS_4 = 4;

	/**
	 * 新增记录
	 * @param urlTxtPath
	 * @return
	 */
	public static UrlTxtPath insert(UrlTxtPath urlTxtPath) {
		try {
			ResultSet resultSet = DBHelper.executeQuery("select if((max(id)+1) is null, 1, (max(id)+1)) nextId from t_url_txt_path;");
			int id = 1;
			if (resultSet.next()) {
				id = resultSet.getInt(1);
			}
			urlTxtPath.setId(id);
			urlTxtPath.setCreateTime(DateUtil.getCurrentDate());
			urlTxtPath.setUpdateTime(DateUtil.getCurrentDate());
			urlTxtPath.setStatus(STATUS_0);
			DBHelper.executeNonQuery(insert_sql, urlTxtPath.getId(), urlTxtPath.getPath(), urlTxtPath.getCreateTime(), urlTxtPath.getUpdateTime(), urlTxtPath.getStatus());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return urlTxtPath;
	}
	
	/**
	 * 更新状态
	 * @param id
	 * @param status
	 * @return
	 */
	public static int updateStatus(Integer id, Integer status) {
		return DBHelper.executeNonQuery(updatre_status_sql, status, DateUtil.getCurrentDate(), id);
	}
	
	/**
	 * 根据状态，获取最早生成的记录
	 * @param status
	 * @return
	 */
	public static UrlTxtPath queryByStatus(Integer status) {
		List<UrlTxtPath> urlTxtPathList = new ArrayList<>();
		ResultSet resultSet = DBHelper.executeQuery(select_sql + " where `status`=? order by create_time asc", status);
		try {
			while (resultSet.next()) {
				UrlTxtPath urlTxtPath = new UrlTxtPath();
				urlTxtPath.setId(resultSet.getInt("id"));
				urlTxtPath.setPath(resultSet.getString("path"));
				urlTxtPathList.add(urlTxtPath);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (urlTxtPathList.size()>0) {
			return urlTxtPathList.get(0);
		}
		return null;
	}
	
	public static void main(String[] args) {
		ResultSet resultSet = DBHelper.executeQuery("select if((max(id)+1) is null, 1, (max(id)+1)) nextId from t_url_txt_path;");
		try {
			if (resultSet.next()) {
				int id = resultSet.getInt(1);
				System.out.println(id);			
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

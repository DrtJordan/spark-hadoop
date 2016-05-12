package com.jary.spark_hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import cn.hadoop.spark_hadoop.domain.UrlLib;

/**
 * @author hjl
 * @date 2016年6月14日 下午6:08:04 
 */
public class TagUtil {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(TagUtil.class);
	
	public static final String SQL_TAG_QUERY = "select tage_id, name from t_tag where 1=1";
	public static final String SQL_MARK_INSERT = "insert into t_tag_mark (tag_id, rela_pk, rela_type, weight, update_time, create_time, status) values (?, ?, ?, ?, ?, ?, ?)";
	
	/**
	 * 性别
	 */
	public static final String Sex_M = "male";
	
	public static final Integer OS_ANDROID = 73;
	public static final Integer OS_IOS = 74;
	public static final Integer OS_MOBILE = 0;
	public static final Integer OS_PC = 72;
	
	public static final Map<String, Integer> proviceMap = new HashMap<>();
	static {
		ResultSet resultSet = DBHelper.executeQuery(SQL_TAG_QUERY + " and parent=146");
		try {
			while (resultSet.next()) {
				proviceMap.put(resultSet.getString("name"), resultSet.getInt("tage_id"));
			}
		} catch (SQLException e) {
			logger.info("初始化标签Map失败！");
			e.printStackTrace();
		}
	}

	/**
	 * @param osType
	 * @return
	 */
	public static Integer getTagIdByOsType(Integer osType) {
		Integer tagId = null;
		switch (osType) {
		case 0:
			tagId = OS_ANDROID;
			break;
		case 1:
			tagId = OS_IOS;
			break;
		case 2:
			tagId = OS_MOBILE;
			break;
		case 3:
			tagId = OS_PC;
			break;

		default:
			tagId = null;
			break;
		}
		return tagId;
	}
	
}

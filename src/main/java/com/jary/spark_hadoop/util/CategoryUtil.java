package com.jary.spark_hadoop.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.hadoop.spark_hadoop.domain.Category;

/**
 * @author hjl
 * @date 2016年8月24日 上午10:38:19 
 */
public class CategoryUtil {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(CategoryUtil.class);
	
	@SuppressWarnings("unused")
	private static final String insert_sql = "INSERT INTO `adPlatform`.`t_category` (`id`, `type_name`, `description`, `parent`, `visible`, `order_no`, `extensions`) VALUES (?, ?, ?, ?, ?, ?, ?);";

	private static final String select_sql = "select `id`, `type_name`, `description`, `parent`, `visible`, `order_no`, `extensions` from `t_category` ";

	/**
	 * 获取待分类的url
	 * 
	 * @return
	 */
	public static List<Category> getCategoryList() {
		List<Category> categoryList = new ArrayList<Category>();
		ResultSet resultSet = DBHelper.executeQuery(select_sql);
		try {
			while (resultSet.next()) {
				Category category = new Category();
				category.setId(resultSet.getInt("id"));
				category.setTypeName(resultSet.getString("type_name"));
				categoryList.add(category);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return categoryList;
	}
}

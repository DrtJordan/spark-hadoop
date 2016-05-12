package com.jary.spark_hadoop.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DBHelper {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(DBHelper.class);
	
	private static Properties jdbcConf = new Properties();;
	
	static {
//		InputStream inputStream;
		try {
//			DBHelper.class.getClassLoader().getResource("jdbc.properties");
//			inputStream = ClassLoader.getSystemResourceAsStream("jdbc.properties");
//			jdbcConf.load(inputStream);//将属性文件流装载到Properties对象中
			//测试环境
//			jdbcConf.put("jdbc.driverClass", "com.mysql.jdbc.Driver");
//			jdbcConf.put("jdbc.url", "jdbc:mysql://210.14.153.156:3306/adPlatform");
//			jdbcConf.put("jdbc.username", "suyue168");
//			jdbcConf.put("jdbc.password", "SUyue168sdk");
			
			//生产环境
			jdbcConf.put("jdbc.driverClass", "com.mysql.jdbc.Driver");
			jdbcConf.put("jdbc.url", "jdbc:mysql://192.168.247.109:3306/telecom");
			jdbcConf.put("jdbc.username", "adsdk");
			jdbcConf.put("jdbc.password", "ADsdk$%^");
		} catch (Exception e) {
			System.out.println("初始化数据库连接参数错误！");
			e.printStackTrace();
		}
	}

    // 此方法为获取数据库连接

    public static Connection getConnection() {
        Connection conn = null;
        try {
//            String driver = "com.mysql.jdbc.Driver"; // 数据库驱动
//            String url = "jdbc:MySQL://127.0.0.1:3306/springmvc";// 数据库
//            String user = "root"; // 用户名
//            String password = "123456"; // 密码
            Class.forName(jdbcConf.getProperty("jdbc.driverClass")); // 加载数据库驱动
            if (null == conn) {
                conn = DriverManager.getConnection(jdbcConf.getProperty("jdbc.url"), jdbcConf.getProperty("jdbc.username"), jdbcConf.getProperty("jdbc.password"));
            }
        } catch (ClassNotFoundException e) {
            System.out.println("Sorry,can't find the Driver!");
            logger.info("Sorry,can't find the Driver!");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

 

    /**
     * 增删改【Add、Del、Update】
     *
     * @param sql
     * @return int
     */

    public static int executeNonQuery(String sql) {
        int result = 0;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            result = stmt.executeUpdate(sql);
        } catch (SQLException err) {
            err.printStackTrace();
            free(null, stmt, conn);
        } finally {
            free(null, stmt, conn);
        }
        return result;
    }

    /**
     * 增删改【Add、Delete、Update】
     *
     * @param sql
     * @param obj
     * @return int
     */
    public static int executeNonQuery(String sql, Object... obj) {
        int result = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < obj.length; i++) {
                pstmt.setObject(i + 1, obj[i]);
            }
            result = pstmt.executeUpdate();
        } catch (SQLException err) {
            err.printStackTrace();
        } finally {
            free(null, pstmt, conn);
        }
        return result;
    }

    /**
     * 增删改【Add、Delete、Update】
     *
     * @param sql
     * @param obj
     * @return int
     */
    public static int[] batchExecuteNonQuery(String sql, List<Object[]> list) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false); 
            pstmt = conn.prepareStatement(sql);
            for (Object[] objects : list) {
                for (int i = 0; i < objects.length; i++) {
                    pstmt.setObject(i + 1, objects[i]);
                }
                pstmt.addBatch();
			}
            int[] result = pstmt.executeBatch();
            conn.commit();
            return result;
        } catch (SQLException err) {
            err.printStackTrace();
        } finally {
            free(null, pstmt, conn);
        }
		return null;
    }

    /**
     * 查【Query】
     *
     * @param sql
     * @return ResultSet
     */
    public static ResultSet executeQuery(String sql) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException err) {
            err.printStackTrace();
            free(rs, stmt, conn);
        }
        return rs;

    }

 

    /**
     * 查【Query】
     *
     * @param sql
     * @param obj
     * @return ResultSet
     */
    public static ResultSet executeQuery(String sql, Object... obj) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < obj.length; i++) {
                pstmt.setObject(i + 1, obj[i]);
            }
            rs = pstmt.executeQuery();
        } catch (SQLException err) {
            err.printStackTrace();
            free(rs, pstmt, conn);
        }
        return rs;
    }

 

    /**
     * 判断记录是否存在
     *
     * @param sql
     * @return Boolean
     */
    public static Boolean isExist(String sql) {
        ResultSet rs = null;
        try {
            rs = executeQuery(sql);
            rs.last();
            int count = rs.getRow();
            if (count > 0) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException err) {
            err.printStackTrace();
            free(rs);
            return false;
        } finally {
            free(rs);
        }
    }

    /**
     * 判断记录是否存在
     *
     * @param sql
     * @return Boolean
     */
    public static Boolean isExist(String sql, Object... obj) {
        ResultSet rs = null;
        try {
            rs = executeQuery(sql, obj);
            rs.last();
            int count = rs.getRow();
            if (count > 0) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException err) {
            err.printStackTrace();
            return false;
        } finally {
            free(rs);
        }
    }

 

    /**
     * 获取查询记录的总行数
     *
     * @param sql
     * @return int
     */
    public static int getCount(String sql) {
        int result = 0;
        ResultSet rs = null;
        try {
            rs = executeQuery(sql);
            rs.last();
            result = rs.getRow();
        } catch (SQLException err) {
            free(rs);
            err.printStackTrace();
        } finally {
            free(rs);
        }
        return result;
    }

    /**
     * 获取查询记录的总行数
     *
     * @param sql
     * @param obj
     * @return int
     */
    public static int getCount(String sql, Object... obj) {
        int result = 0;
        ResultSet rs = null;
        try {
            rs = executeQuery(sql, obj);
            rs.last();
            result = rs.getRow();
        } catch (SQLException err) {
            err.printStackTrace();
        } finally {
            free(rs);
        }
        return result;
    }

    /**
     * 释放【ResultSet】资源
     *
     * @param rs
     */
    public static void free(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    /**
     * 释放【Statement】资源
     *
     * @param st
     */
    public static void free(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    /**
     * 释放【Connection】资源
     *
     * @param conn
     */
    public static void free(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    /**
     * 释放所有数据资源
     *
     * @param rs
     * @param st
     * @param conn
     */
    public static void free(ResultSet rs, Statement st, Connection conn) {
        free(rs);
        free(st);
        free(conn);
    }
}
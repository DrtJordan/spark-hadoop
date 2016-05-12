package com.jary.spark_hadoop.ad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import cn.hadoop.spark_hadoop.dao.UrlTxtPathDao;
import cn.hadoop.spark_hadoop.domain.UrlLib;
import cn.hadoop.spark_hadoop.domain.UrlTxtPath;
import cn.hadoop.spark_hadoop.util.DateUtil;
import cn.hadoop.spark_hadoop.util.FileUtils;
import cn.hadoop.spark_hadoop.util.HdfsFileSystemUtil;
import cn.hadoop.spark_hadoop.util.UrlLibUtil;

/**
 * 根据t_url_lib的url，抓取网页文本并保存为txt文件
 * @author hjl
 * @date 2016年8月24日 下午3:00:33 
 */
public class CrawlUrlTxtProcess {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(CrawlUrlTxtProcess.class);
	
	public static void main(String[] args) {
		long start = new Date().getTime();
		logger.info("##########start（" + start + "）.............");
		int count = UrlLibUtil.countByStatus(UrlLibUtil.AD_CATEGORY_UNKNOWN_PUSH);
		if (count >= UrlLibUtil.BATCH_NUM * UrlLibUtil.THREAD_NUM) {

//	    	List<UrlLib> urlLibList = UrlLibUtil.getUrlLibList();
//	    	UrlLibUtil.updateUrlStatus(urlLibList);
//	    	UrlLibUtil.saveUrlTxt(urlLibList, path);
			String path = UrlLibUtil.getTxtPath(DateUtil.getCurrentTimeMillis());
	    	try {
	    		List<CrawlUrlTxtThread> threadList = new ArrayList<CrawlUrlTxtThread>();
	    		for (int i = 0; i < UrlLibUtil.THREAD_NUM; i++) {
	    	    	List<UrlLib> urlLibList = UrlLibUtil.getUrlLibList();
	    	    	UrlLibUtil.updateUrlStatus(urlLibList);
	            	CrawlUrlTxtThread crawlUrlTxtThread = new CrawlUrlTxtThread(urlLibList, path);
	            	crawlUrlTxtThread.start();
	    			threadList.add(crawlUrlTxtThread);
				}
	    		for (CrawlUrlTxtThread crawlUrlTxtThread : threadList) {
	    			crawlUrlTxtThread.join();
				}
				
				UrlTxtPath urlTxtPath = new UrlTxtPath();
				urlTxtPath.setPath(path);
				urlTxtPath = UrlTxtPathDao.insert(urlTxtPath);

		    	//上传至HDFS
				HdfsFileSystemUtil.copyFile(path, path);
				UrlTxtPathDao.updateStatus(urlTxtPath.getId(), UrlTxtPathDao.STATUS_1);
			} catch (Exception e) {
				logger.info("上传文件至hdfs失败!");
				e.printStackTrace();
			}
	    	FileUtils.delete(path);
		} else {
			logger.info("待处理的url不足一次处理的数量，跳过！");
		}
		long end = new Date().getTime();
		logger.info("##########end（" + end + "）.............");
		System.exit(0);
	}
}

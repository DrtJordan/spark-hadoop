package com.jary.spark_hadoop.ad;

import java.util.List;

import cn.hadoop.spark_hadoop.domain.UrlLib;
import cn.hadoop.spark_hadoop.util.UrlLibUtil;

/**
 * @author hjl
 * @date 2016年9月5日 上午10:19:54 
 */
public class CrawlUrlTxtThread extends Thread {
	
	private List<UrlLib> urlLibList;
	
	private String savePath;
	
	/**
	 * @param urlLibList
	 * @param savePath
	 */
	public CrawlUrlTxtThread(List<UrlLib> urlLibList, String savePath) {
		super();
		this.urlLibList = urlLibList;
		this.savePath = savePath;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		UrlLibUtil.saveUrlTxt(urlLibList, savePath);
	}

	/**
	 * @return the urlLibList
	 */
	public List<UrlLib> getUrlLibList() {
		return urlLibList;
	}

	/**
	 * @param urlLibList the urlLibList to set
	 */
	public void setUrlLibList(List<UrlLib> urlLibList) {
		this.urlLibList = urlLibList;
	}

	/**
	 * @return the savePath
	 */
	public String getSavePath() {
		return savePath;
	}

	/**
	 * @param savePath the savePath to set
	 */
	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}

}

package com.jary.spark_hadoop.domain;

import java.io.Serializable;

/**
 * @author ll
 * @date 2016年5月3日 下午1:48:22 
 */
public class UrlLib implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 136006441601934228L;

	private String urlMd5;

	private String host;

	private String url;

	private String keyworld;

	private Integer category;

	private Integer adCategory;

	private Integer status;

	/**
	 * @return the urlMd5
	 */
	public String getUrlMd5() {
		return urlMd5;
	}

	/**
	 * @param urlMd5 the urlMd5 to set
	 */
	public void setUrlMd5(String urlMd5) {
		this.urlMd5 = urlMd5;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the keyworld
	 */
	public String getKeyworld() {
		return keyworld;
	}

	/**
	 * @param keyworld the keyworld to set
	 */
	public void setKeyworld(String keyworld) {
		this.keyworld = keyworld;
	}

	/**
	 * @return the category
	 */
	public Integer getCategory() {
		return category;
	}

	/**
	 * @param category the category to set
	 */
	public void setCategory(Integer category) {
		this.category = category;
	}

	/**
	 * @return the adCategory
	 */
	public Integer getAdCategory() {
		return adCategory;
	}

	/**
	 * @param adCategory the adCategory to set
	 */
	public void setAdCategory(Integer adCategory) {
		this.adCategory = adCategory;
	}

	/**
	 * @return the status
	 */
	public Integer getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(Integer status) {
		this.status = status;
	}
	
}

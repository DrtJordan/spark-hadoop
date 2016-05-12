package com.jary.spark_hadoop.domain;

/**
 * @author hjl
 * @date 2016年6月15日 下午1:35:50 
 */
public class IpInfoResult {

	private String code;
	
	private IpInfo data;

	/**
	 * @return the code
	 */
	public String getCode() {
		return code;
	}

	/**
	 * @param code the code to set
	 */
	public void setCode(String code) {
		this.code = code;
	}

	/**
	 * @return the data
	 */
	public IpInfo getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(IpInfo data) {
		this.data = data;
	}
	
}

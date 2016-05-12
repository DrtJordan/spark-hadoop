package com.jary.spark_hadoop.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

/**
 * @author ll
 * @date 2016年5月6日 上午10:35:05 
 */
public class StringUtil {

	private static final String charset = "utf-8";

	public static final String[] areaKeyword = {"省", "市", "自治区", "特别行政区"};

	public static String getHost(String url) {
		String host = "";
		// 用户访问来源地址
		if (!StringUtils.isEmpty(url)) {
			if (!url.contains("http://") && !url.contains("https://")) {
				url = "http://" + url;
			}
			if (url.indexOf("http://m.baidu.com/") == 0) {
				// 主要判断百度的阅读模式，获取真实来源地址
				String src = getURLParameter(url, "src", "") + "";
				if (!StringUtils.isEmpty(src)) {
					url = (src);
				}
			}
			host = url.substring(url.indexOf("//") + 2);
			if (host.indexOf("/") > 0) {
				host = host.substring(0, host.indexOf("/"));
			}
		}
		return host;
	}
	
	/**
	 * 获取URL，参数
	 * @param url
	 * @param name
	 * @return
	 */
	public static Object getURLParameter(URL url,String name,Object defaultValue){
		List<NameValuePair> paramList = URLEncodedUtils.parse(url.getQuery(), Charset.forName(charset));
		for (NameValuePair nameValuePair : paramList) {
			if(nameValuePair.getName().equals(name)){
				return nameValuePair.getValue();
			}
		}
		return defaultValue;
	}
	/**
	 * 获取URL，参数
	 * @param url
	 * @param name
	 * @return
	 */
	public static Object getURLParameter(String url,String name,Object defaultValue){
		try {
			return getURLParameter(new URL(url), name, defaultValue);
		} catch (MalformedURLException e) {
			return null;
		}
	}

	/**
	 * @param province
	 * @return
	 */
	public static String removeAreaKeyWord(String area) {
		if (area!=null) {
			for (int i = 0; i < areaKeyword.length; i++) {
				area = area.replace(areaKeyword[i], "");
			}
		}
		return area;
	}
	public static boolean isEmpty(Object obj) {
		return obj == null ? true : String.valueOf(obj).trim().length() == 0;
	}

	public static String append2length(String str, int length) {
		if (isEmpty(str) || str.length()>=length) {
			return str;
		}
		StringBuffer sb = new StringBuffer(str);
		for (int i=0; i<length-str.length(); i++) {
			sb.append(" ");
		}
		return sb.toString();
	}

	public static String append2length(String str, int length, String str2) {
		if (isEmpty(str) || str.length()>=length) {
			return str;
		}
		StringBuffer sb = new StringBuffer(str);
		for (int i=0; i<length-str.length(); i++) {
			sb.append(str2);
		}
		return sb.toString();
	}
}

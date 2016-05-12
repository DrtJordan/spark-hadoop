package com.jary.spark_hadoop.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.alibaba.fastjson.JSON;

import cn.hadoop.spark_hadoop.domain.IpInfoResult;

/**
 * 
 * @author jary0524
 * @date 2016年5月3日 上午9:47:51 
 */
public class JsoupUtil {
	/**
	* Logger for this class
	*/
	private static final Logger logger = Logger.getLogger(JsoupUtil.class);
	
	/**
	 * 任意个汉字
	 */
	public static final String regex_CN = "([\u4E00-\u9FA5]+)";
	public static final String regex_CN_NUM = "([\u4E00-\u9FA50-9]+)";
	
	public static final String IP_API_TAOBAO = "http://ip.taobao.com/service/getIpInfo.php?ip=";
	

	/**
	 * 获取网页源码
	 * @param url
	 * @return
	 */
	public static String getContent (String url) {
		if (StringUtils.isEmpty(url)) {
			return "";
		}
		//解析Url获取Document对象
		//伪装成浏览器
		Document document = getDocument(url);
		return document.body().html();
	}
	public static Document getDocument (String url) {
		Document document = null;
		try {
            //解析Url获取Document对象
			//伪装成浏览器
			document = Jsoup.connect(url)
					.timeout(5000)
					.header("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0")//Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)
					.get();
        } catch (IOException e) {
        	logger.info("解析网页出错！url:" + url);
        }
		return document;
	}
	/**
	 * 获取网页源码中的中文
	 * @param url
	 * @return
	 */
	public static String getChinese(String url) {
		if (StringUtils.isEmpty(url)) {
			return "";
		}
		//解析Url获取Document对象
		//伪装成浏览器
		Document document = getDocument(url);
//        //获取网页源码文本内容
//        System.out.println(document.toString());
		return getChinese(document);
	}
	public static String getChinese(Document document) {
		if (document==null) {
			return "";
		}
		StringBuffer sb = new StringBuffer("");
		try {
			Pattern p = Pattern.compile(regex_CN);
			Matcher m = p.matcher(document.toString());
			while (m.find()) {
				sb.append(" " + m.group(0));
			}
		} catch (Exception e) {
            e.printStackTrace();
		}
		return sb.toString();
	}
	
	/**
	 * 获取网页文本
	 * @param url
	 * @return
	 */
	public static String getText(String url) {
		StringBuffer sb = new StringBuffer("");
		if (StringUtils.isEmpty(url)) {
			return "";
		}
		try {
			//解析Url获取Document对象
			//伪装成浏览器
			Document document = getDocument(url);
			String text = document.body().text();
			//若无文本，取网页中的中文
			if (StringUtils.isEmpty(text)) {
				text = getChinese(document);
			}
			sb.append(text);
		} catch (Exception e) {
			
		}
		return sb.toString();
	}


	public static Set<String> crawlUrlByIndex(String indexUrl, List<String> containList) {

		Set<String> urlList = new HashSet<>();
		try {
			//1、抓取url
	    	Document doc = JsoupUtil.getDocument(indexUrl);
	    	Elements hrefs = doc.select("a[href]");
			//2、转化为Object
	    	for(Element elem:hrefs){
	    		String url = elem.attr("abs:href");
				if (StringUtils.isEmpty(url)) {
					continue;
				}
				boolean contain = true;
				if (containList!=null && containList.size()>0) {
					for (String containUrl : containList) {
						if (!url.contains(containUrl)) {
							contain = false;
							break;
						}
					}
				}
				if (contain) {
	                urlList.add(url);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return urlList;
	}

  	public static void main(String[] args) {
//        //解析Url获取Document对象
        String text = getText("http://finance.sina.com.cn/china/gncj/2016-07-19/doc-ifxuapvw2304231.shtml");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("##############################################");
        System.out.println("text: " + text);
  		
//  		StringBuffer sb = new StringBuffer();
//        //解析Url获取Document对象
//  		String text = getText("http://222.186.50.160:801/new.html");
//		sb.append(text);
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("##############################################");
//        System.out.println("text: " + sb.toString());
  		
//  		String result = JsoupUtil.getContent(JsoupUtil.IP_API_TAOBAO+ "113.99.15.127");
//  		IpInfoResult ipInfoResult = (IpInfoResult) JSON.parseObject(result, IpInfoResult.class);
//  		System.out.println(ipInfoResult);
	}
	
}

package com.jary.spark_hadoop.util;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;


/**
 * 根据 user agent string 判断用户的平台、浏览器 参考资料
 * *****************************************************************************
 * *********************************************************************
 * 
 * 台式机
 * 
 * Linux Ubuntu Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.2pre)
 * Gecko/20100225 Ubuntu/9.10 (karmic) Namoroka/3.6.2pre
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Linux
 * Mandriva 2008.1 Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1)
 * Gecko/2008072403 Mandriva/3.0.1-1mdv2008.1 (2008.1) Firefox/3.0.1
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Linux
 * suSE 10.1 Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.3) Gecko/20060425
 * SUSE/1.5.0.3-7 Firefox/1.5.0.31
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Windows
 * XP SP3 Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1)
 * Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Windows
 * Vista Mozilla/5.0 (Windows; U; Windows NT 6.1; nl; rv:1.9.2.13)
 * Gecko/20101203 Firefox/3.6.13 Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US;
 * rv:1.9.2.6) Gecko/20100625 Firefox/3.6.6 (.NET CLR 3.5.30729)
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- windows
 * 2000 Mozilla/5.0 (Windows; U; Windows NT 5.0; en-GB; rv:1.8.1b2)
 * Gecko/20060821 Firefox/2.0b2
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Windows
 * 7 Mozilla/5.0 (Windows NT 6.1; WOW64; rv:14.0) Gecko/20100101 Firefox/14.0.1
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Windows
 * Server 2008 Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.5)
 * Gecko/20091102 Firefox/3.5.5 (.NET CLR 3.5.30729)
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- iMac
 * OSX 10.7.4 Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:13.0)
 * Gecko/20100101 Firefox/13.0.1
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Mac OS
 * X Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2.9)
 * Gecko/20100824 Firefox/3.6.9
 * -----------------------------------------------------------------------------
 * ---------------------------------------------------------------------
 * 
 * 手持设备
 * 
 * iPad Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us)
 * AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b
 * Safari/531.21.10
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- iPad 2
 * Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X; en-us) AppleWebKit/534.46
 * (KHTML, like Gecko) Version/5.1 Mobile/9B176 Safari/7534.48.3
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- iPhone
 * 4 Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_0 like Mac OS X; en-us)
 * AppleWebKit/532.9 (KHTML, like Gecko) Version/4.0.5 Mobile/8A293
 * Safari/6531.22.7
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- iPhone
 * 5 Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46
 * (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3
 * -----------------------------------------------------------------------------
 * --------------------------------------------------------------------- Android
 * Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91)
 * AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1
 * *****************************************************************************
 * *********************************************************************
 * 
 * @author Defonds
 */
public class UserAgentUtil {

	public static final String BROWSER_IE = "IE";
	public static final String BROWSER_FF = "FF";

	public static Integer getBrowserName(String userAgent) {
		String agent = userAgent.toLowerCase();
		if (agent.indexOf("opera") > 0) {
			return 82;
		} else if (agent.indexOf("360se") > 0) {
			return 78;
		} else if (agent.indexOf("metasr") > 0) {
			return 79;
		} else if (agent.indexOf("ucweb") > 0) {
			return 83;
//		} else if (agent.indexOf("the world") > 0) {
//			return "The World";//世界之窗
		} else if (agent.indexOf("maxthon") > 0) {
			return 84;//傲游
		} else if (agent.indexOf("tencenttraveler") > 0) {
			return 80;//腾讯TT
		} else if (agent.indexOf("qqbrowser") > 0) {
			return 80;//QQ
		} else if (agent.indexOf("firefox") > 0) {
			return 77;
//		} else if (agent.indexOf("webkit") > 0) {
//			return "webkit";
		} else if (agent.indexOf("msie") > 0) {
			return 75;
		} else if (agent.indexOf("chrome") > 0) {
			return 76;
		} else if (agent.indexOf("safari") > 0) {
			return -1;
		} else {
			return -1;
		}
	}
	
}
package com.jary.spark_hadoop.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @Description: md5编码解码工具 
 * @author jary0524
 * @date 2015年9月29日 上午10:48:53 
 */
public class Md5Util {

    private static final char hexDigits[]={'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    public static final String MD5 = "MD5";
	
	/**
	 * 生成md5校验码
	 * @param srcContent 需要加密的数据
	 * @return 加密后的md5校验码。出错则返回null。
	 */
	public static String MD5(String srcContent){
		return MD5(srcContent.getBytes());//str.getBytes("UTF-8")
	}
	
	/**
	 * 生成md5校验码
	 * @param srcContent 需要加密的数据
	 * @return 加密后的md5校验码。出错则返回null。
	 */
	public static String MD5(byte[] content) {
		if (content == null)	{
			return null;
		}
		String strDes = null;
		try	{
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(content);
			strDes = byteArrayHexString(md5.digest()); // to HexString
		} catch (NoSuchAlgorithmException e) {
			return null;
		}
		return strDes;
	}

	/**
	 * 将字节数组转换成十六进制的字符串形式
	 * @param byteArray
	 * @return
	 */
	public static String byteArrayHexString(byte[] byteArray) {
        int j = byteArray.length;
        char str[] = new char[j * 2];
        int k = 0;
        for (int i = 0; i < j; i++) {
            byte byte0 = byteArray[i];
            str[k++] = hexDigits[byte0 >>> 4 & 0xf];
            str[k++] = hexDigits[byte0 & 0xf];
        }
        return new String(str);
	}

	public static void main(String[] args) {
		System.out.println(Md5Util.MD5("adddsfdssf"));
	}
}

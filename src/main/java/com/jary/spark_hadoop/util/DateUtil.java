package com.jary.spark_hadoop.util;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;


/**
 * @Description: 日期工具类
 * @author jary0524
 * @date 2015年7月31日 上午10:18:44 
 */
public class DateUtil {
	/**
	 * 日期类型 年
	 */
	public static final String YEAR="year";
	/**
	 * 日期类型 天
	 */
	public static final String DAY="day";
	/**
	 * 日期类型 月
	 */
	public static final String MONTH="month";
	/**
	 * 日期类型 时
	 */
	public static final String HOUR="hour";
	/**
	 * 日期类型 分
	 */
	public static final String MINUTE="minute";
	/**
	 * 日期类型 秒
	 */
	public static final String SECOND="second";
	/**
	 * 升序
	 */
	public static final String ASC="asc";
	/**
	 * 降序
	 */
	public static final String DESC="desc";

	public static final String TIMEOUT="timeout";
	
	public static final String DATE_YYYYMM = "yyyyMM";
	public static final String DATE_YYYYMMDD = "yyyyMMdd";
	public static final String DATE_YYYY_MM_DD = "yyyy-MM-dd";
	public static final String DATE_TIME = "yyyy-MM-dd HH:mm:ss";
	
	
//  做一个简单的压力测试，方法一最慢，方法三最快，但是就算是最慢的方法一性能也不差，一般系统方法一和方法二就可以满足，所以说在这个点很难成为你系统的瓶颈所在。从简单的角度来说，建议使用方法一或者方法二，如果在必要的时候，追求那么一点性能提升的话，可以考虑用方法三，用ThreadLocal做缓存。
	/**
	 * 标准时间格式
	 */
	public static final SimpleDateFormat SDF_DATE_TIME = new SimpleDateFormat(DATE_TIME);
	
	/**
	 * 标准时间格式
     * @param date
     * @return
     * @throws ParseException
     */
    public static String formatDate(Date date)throws ParseException{
        synchronized(SDF_DATE_TIME){
            return SDF_DATE_TIME.format(date);
        }  
    }
    
	/**
	 * 标准时间格式
     * @param strDate
     * @return
     * @throws ParseException
     */
    public static Date parse(String strDate) throws ParseException{
        synchronized(SDF_DATE_TIME){
            return SDF_DATE_TIME.parse(strDate);
        }
    }
	

	
//	/**
//	 * 使用ThreadLocal, 也是将共享变量变为独享
//	 */
//	private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>();
//	
//	public static DateFormat getDateFormat() {  
//        DateFormat df = threadLocal.get();  
//        if(df==null){  
//            df = new SimpleDateFormat(DATE_TIME);  
//            threadLocal.set(df);  
//        }  
//        return df;  
//    }  
//
//    public static String formatDate(Date date) throws ParseException {
//        return getDateFormat().format(date);
//    }
//
//    public static Date parse(String strDate) throws ParseException {
//        return getDateFormat().parse(strDate);
//    }

    
	/**
	 * 字符串转换为日期
	 * @param source 待转换的日期字符串,不能为空
	 * @param style 日期格式,参见isDateStyle方法中的日期格式说明
	 * @return java.util.Date 转换后的日期
	 */
	public static java.util.Date parseDate (String source, String pattern){
		if (source==null || source.trim().equals("")) {
			return null;
		}
		source = source.trim();
		pattern = pattern.trim();
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.parse(source,new ParsePosition(0));
	}
	
	/**
	 * 字符串转换为日期
	 * @param source 待转换的日期字符串,不能为空
	 * @param style 日期格式,参见isDateStyle方法中的日期格式说明
	 * @return java.util.Date 转换后的日期
	 */
	public static java.sql.Date strToSQLDate (String source,String style){
		if (source==null || source.trim().equals(""))
			return null;
		source=source.trim();
		style=style.trim();
		SimpleDateFormat sdf=new SimpleDateFormat(style);
		return new java.sql.Date(sdf.parse(source,new ParsePosition(0)).getTime());
	}
	
	/**
	 * 转换日期为指定格式的字符串
	 * @param date0 待转换的日期
	 * @param style 日期格式,参见isDateStyle方法中的日期格式说明
	 * @return java.lang.String 如果date0为空,返回空字符串
	 */
	public static String formatDate (java.util.Date date0,String style){
		if (date0==null){
			return "";
		}
		SimpleDateFormat sdf=new SimpleDateFormat(style);
		return sdf.format(date0);
	}

	/**
	 * 转换日期为指定格式的字符串
	 * @param date0 待转换的日期
	 * @param style 日期格式,参见isDateStyle方法中的日期格式说明
	 * @return java.lang.String 如果date0为空,返回空字符串
	 */
	public static String formatDate (java.sql.Date date0,String style){
		if (date0==null){
			return "";
		}
		return formatDate(new java.util.Date(date0.getTime()),style);
	}

	/**
	 * 转换日期为指定格式的字符串
	 * @param date0 待转换的日期
	 * @param style 日期格式,参见isDateStyle方法中的日期格式说明
	 * @return java.lang.String 如果date0为空,返回空字符串
	 */
	public static String formatDate (java.sql.Timestamp date0,String style){
		if (date0==null){
			return "";
		}
		return formatDate(new java.util.Date(date0.getTime()),style);
	}

	  /**
	   * 
	   * @Methodname: dateFormat
	   * @Discription: 格式化日期
	   * @param date
	   *          日期
	   * @param format
	   *          日期格式，参考系统常量
	   * @Return: Date
	   * @Throws
	   * 
	   */
	public static Date dateFormat(Date date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			String dateString = sdf.format(date);
			date = sdf.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
//			throw new BaseException(e.getMessage());
		}
		return date;
	}
	  
	/**
	 * util.Date转换成sql.Date日期
	 * @param source 待转换的util.Date日期,如果为空，取当前日期
	 * @return java.sql.Date 转换后的日期
	 */
	public static java.sql.Date utilToSQLDate (java.util.Date utilDate){
		if (utilDate==null)
			utilDate = new java.util.Date();
		return new java.sql.Date(utilDate.getTime());
	}	
	/**
	 * util.Date转换成sql.Date日期
	 * @param source 待转换的util.Date日期,如果为空，取当前日期
	 * @return java.sql.Date 转换后的日期
	 */
	public static java.util.Date sqlToUtilDate (java.sql.Date sqlDate){
		if (sqlDate==null)
			return new java.util.Date();
		String strDate = formatDate(sqlDate,"yyyyMMdd");
		return parseDate(strDate,"yyyyMMdd");
	}
	  /**
	   * 
	   * @Methodname: changeDate
	   * @Discription: 改变日期
	   * @param date
	   *          日期
	   * @param type
	   *          改变日期的类型，参考系统常量
	   * @param number
	   *          改变日期的数目
	   * @Return: Date
	   * @Throws
	   * 
	   */
	public static Date changeDate(Date date, String type, int number) {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		if (DateUtil.YEAR.equals(type))
			calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) + number);
		else if (DateUtil.MONTH.equals(type))
			calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) + number);
		else if (DateUtil.DAY.equals(type))
			calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + number);
		else if (DateUtil.HOUR.equals(type))
			calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + number);
		else if (DateUtil.MINUTE.equals(type))
			calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) + number);
		else if (DateUtil.SECOND.equals(type))
			calendar.set(Calendar.SECOND, calendar.get(Calendar.SECOND) + number);
		else
			throw new RuntimeException("error");
		
		return calendar.getTime();
	}

  
    /**
     * @Methodname: getCurrentDate
     * @Discription: 获取当前服务时间,分布式时需要改此方法
	 * @return
	 */
	public static java.util.Date getCurrentDate() {
		return new java.util.Date();
	}

    /**
     * 
     * @Methodname: getCurrentTimeMillis
	 * @return
	 */
	public final static long getCurrentTimeMillis() {
		return System.currentTimeMillis();
	}
  
  /**
   * @Methodname: getWeekDayString
   * @Discription: 获取某天是星期几
   * @return  星期几
   * @Return: String
   * @Throws 
   */
  public static String getWeekDayString(Date today )  {
	  String weekString = "";
	  final String dayNames[] = {"星期日","星期一","星期二","星期三","星期四","星期五","星期六"};
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(today); 
	  int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
	  weekString = dayNames[dayOfWeek - 1];
	  return weekString;
  }
  /**
   * 获取某日期的星期几
   */
  public static int getWeekDay(Date today )  {
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(today); 
	  return calendar.get(Calendar.DAY_OF_WEEK);
  }
  /**
   * 
   * @Methodname: getYMDByDate
   * @Discription: 得到日期的年月日(包括时分秒)
   * @param date
   * @Return: Map<String,String>，第一个参数是DateUtil.YEAR（MONTH,DAY），第二个参数就是值
   * @Throws
   *
   */
  public static Map<String,String> getYMDByDate(Date date) {
	  Calendar calendar = new GregorianCalendar();
	  calendar.setTime(date);
	  Map<String,String> ymd=new HashMap<String, String>();
	  int year=calendar.get(Calendar.YEAR);
	  int month=calendar.get(Calendar.MONTH)+1;
	  int day=calendar.get(Calendar.DATE);
	  int hour=calendar.get(Calendar.HOUR);
	  int minute=calendar.get(Calendar.MINUTE);
	  int second=calendar.get(Calendar.SECOND);
	  ymd.put(DateUtil.YEAR, String.valueOf(year));
	  ymd.put(DateUtil.MONTH, String.valueOf(month));
	  ymd.put(DateUtil.DAY, String.valueOf(day));
	  ymd.put(DateUtil.HOUR, String.valueOf(hour));
	  ymd.put(DateUtil.MINUTE, String.valueOf(minute));
	  ymd.put(DateUtil.SECOND, String.valueOf(second));
	  return ymd;
  }
  
   /**            
	 * 计算两个日期之间相差的天数 ,注意，时间大的在前面，时间小的在后面
	 * @param date1    
	 * @param date2            
	 */           
	 public static int diffDate(Date date1, Date date2) {                   
		 Calendar calendar = Calendar.getInstance();                   
		 calendar.setTime(date1);                   
		 Calendar calendar2 = Calendar.getInstance();                   
		 calendar2.setTime(date2);                      
		 return diffDate(calendar, calendar2);           
	 }     
	 /**
	  *  注意，时间大的在前面，时间小的在后面
	  */
	 public static int diffDate(Calendar d1, Calendar d2) {                   
		 if (d1.after(d2)) {                           
			 Calendar swap = d1;                           
			 d1 = d2;                           
			 d2 = swap;                   
		 }                   
		 int days = d2.get(Calendar.DAY_OF_YEAR) - d1.get(Calendar.DAY_OF_YEAR);                   
		 int y2 = d2.get(Calendar.YEAR);                   
		 if (d1.get(Calendar.YEAR) != y2) {                           
			 d1 = (Calendar) d1.clone();                           
			 do {                                   
				 days += d1.getActualMaximum(Calendar.DAY_OF_YEAR);// 得到当年的实际天数                                   
				 d1.add(Calendar.YEAR, 1);                           
			 } while (d1.get(Calendar.YEAR) != y2);                   
		 }                   
		 return days;           
	 }
	 
	  /**
	   * 
	   * @Methodname: isMorning
	   * @Discription: 判断时间是否为上午
	   * @param date  时间
	   * @Return: boolean
	   * @Throws
	   * 
	   */
	  public static boolean isMorning(Date date) {
		 Calendar cal=Calendar.getInstance();
		 cal.setTime(date);
		 if(cal.get(Calendar.HOUR_OF_DAY) >= 12) {
			 return false;
		 }
		 return true;
	  }
	  
	/**
	 * 获取与某月间隔n个月的日期表示
	 * @param currentMonth 带4位年份的月
	 * @param n 若n大于0,表示currentMonth之后(将来的)的月份,若n小于0,表示currentMonth之前(过去的)的月份
	 * @return 返回带4位年份的下一个月字符串表示,4位年份+2位月份(不足两位前面补0)
	 */
	public static String addMonth (String currentMonth,int n){
		int year=Integer.parseInt(currentMonth.substring(0,4));
		int month=Integer.parseInt(currentMonth.substring(4));
		month=year*12+month+n;
		year=month/12;
		month=month-year*12;
		if (month==0){
			year=year-1;
			month=12;
		}
		return new Integer(year*100+month).toString();
	}

	/**
	 * 获取与某月间隔n天的日期字符串表示,格式为yyyyMMdd
	 * @param currentDate 格式位yyyyMMdd的日期表示
	 * @param n 若n大于0,表示currentDate之后(将来的)的日期,若n小于0,表示currentDate之前(过去的)的日期
	 * @return 返回带4位年份的下一个月字符串表示,4位年份+2位月份(不足两位前面补0)
	 */
	public static String addDateString (String currentDate,int n){
		Calendar cl=Calendar.getInstance();
		cl.setTime(parseDate(currentDate,"yyyyMMdd"));
		cl.add(Calendar.DATE,n);
		return formatDate(cl.getTime(),"yyyyMMdd");
	}

	/**
	 * 获取与某月间隔n天的日期字符串表示,格式为style
	 * @param currentDate 格式位yyyyMMdd的日期表示
	 * @param n 若n大于0,表示currentDate之后(将来的)的日期,若n小于0,表示currentDate之前(过去的)的日期
	 * @return 返回带4位年份的下一个月字符串表示,4位年份+2位月份(不足两位前面补0)
	 */
	public static String addDateString (String currentDate,int n,String style){
		Calendar cl=Calendar.getInstance();
		cl.setTime(parseDate(currentDate,"yyyyMMdd"));
		cl.add(Calendar.DATE,n);
		return formatDate(cl.getTime(),style);
	}
	/**
	 * 获取与某月间隔n天的日期表示
	 * @param currentDate 日期
	 * @param n 若n大于0,表示currentDate之后(将来的)的日期,若n小于0,表示currentDate之前(过去的)的日期
	 * @return java.util.Date 返回日期
	 */
	public static java.util.Date addDate (java.util.Date currentDate,int n){
		Calendar cl=Calendar.getInstance();
		cl.setTime(currentDate);
		cl.add(Calendar.DATE,n);
		return cl.getTime();
	}
	
	

	
	private final static SimpleDateFormat sdfYear = new SimpleDateFormat("yyyy");

	private final static SimpleDateFormat sdfDay = new SimpleDateFormat(
			"yyyy-MM-dd");
	
	private final static SimpleDateFormat sdfDays = new SimpleDateFormat(
	"yyyyMMdd");

	private final static SimpleDateFormat sdfTime = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/**
	 * 获取YYYY格式
	 * 
	 * @return
	 */
	public static String getYear() {
		return sdfYear.format(new Date());
	}

	/**
	 * 获取YYYY-MM-DD格式
	 * 
	 * @return
	 */
	public static String getDay() {
		return sdfDay.format(new Date());
	}
	
	/**
	 * 获取YYYYMMDD格式
	 * 
	 * @return
	 */
	public static String getDays(){
		return sdfDays.format(new Date());
	}

	/**
	 * 获取YYYY-MM-DD HH:mm:ss格式
	 * 
	 * @return
	 */
	public static String getTime() {
		return sdfTime.format(new Date());
	}
}

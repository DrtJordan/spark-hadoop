package com.jary.spark_hadoop.util;



public class UMConstants {// 渠道
	
	public static final String API_METHOD = "method";
	public static final String API_ACCESSKEY = "accesskey";
	public static final String API_SIGN = "sign";
	public static final String API_PARAMS = "params";
	public static final String API_NODE_CODE = "nodecode";
	public static final String API_TIMESTAMP = "timestamp";
	public static final String API_REDIRECT_COUNT = "redirectcount";
	public static final String API_RESID = "resid";
	public static final String API_ADINFO = "adinfo";
	public static final String API_EXCLUDE_RULES = "excluderules";
	
	/**
	 * 搜索平台：百度
	 */
	public static final String SEARCH_PF_CODE_BAIDU = "baidu";
	/**
	 * 搜索平台：淘宝
	 */
	public static final String SEARCH_PF_CODE_TAOBAO= "taobao";
	

	public static final String COUNT = "count";
	public static final String CHANNEL = "channel";

	public static final String ADMIN_ROLE_ID = "business";
	public static final String GAGENT_ROLE_ID = "gagent";
	
	public static final String SESSION_UUID = "sid";

	// 上报日期
	public static final String REPORT_DATE = "reportdate";
	// 当天日期
	public static final String CURRENT_DATE = "currentdate";
	// 当天推送的数量
	public static final String CURRENT_DATE_PUSH_COUNT = "currentdatepushcount";
	// 应用内最后推送时间
	public static final String LAST_PUSH_TIME = "lastpushtime";
	// 通知栏最后推送时间
	public static final String NOTIFICATION_LAST_PUSH_TIME = "notificationlastpushtime";
	// 橱窗广告最后推送时间
	public static final String WINDOW_LAST_PUSH_TIME = "windowlastpushtime";
	// 成功数量
	public static final String SUCCESS_RECORD = "successrecord";
	// 失败数量
	public static final String FAILED_RECORD = "failedrecord";
	// 内容

	// 用户手机唯一标志，android用imei,ios 用uuid
	public static final String USER_KEY = "userkey";
	// 移动设备国际辨识码
	public static final String IMEI = "phoneid";
	// SIM客户识别模块
	public static final String IMSI = "simid";
	//是否为结算用户
	public static final String ISSETTLE = "issettle";
	// 手机类别，0为android，1为iphone，2，为ipad
	public static final String OS_TYPE = "ostype";
	// 手机Mac 地址
	public static final String MAC_ADDRESS = "macaddress";
	// 操作系统版本
	public static final String OS_VERSION = "osversion";
	// 手机型号，比如iPhone3,1
	public static final String CLIENT_MODEL = "model";
	// 客户端屏幕320*480
	public static final String SCREEN_SIZE = "screensize";
	// 应用版本
	public static final String APP_VERSION = "appversion";
	// 客户端语言
	public static final String LANG = "lang";
	// 注册ID，服务端生成
	public static final String REGISTER_ID = "registerid";
	// 注册随机密码
	public static final String PASSWORD = "password";
	// 是否有广告
	public static final String HAS_AD = "hasad";

	public static final String CREATE_DATE = "createdate";
	// 网络类型
	public static final String NETWORK_TYPE = "networktype";

	public static final String APP_KEY = "appkey";

	public static final String BATCH_STEP = "batchstep";
	// URL
	public static final String URL = "url";
	// 插屏模板
	public static final String SCREEN_TEMPLATE = "screentemplate";
	// 橱窗模板
	public static final String WINDOW_TEMPLATE = "windowtemplate";
	// 上报步骤
	public static final String STEP = "step";
	public static final String NETTWORK_TYPE = "networktype";
	public static final String USER_TYPE = "usertype";
	public static final int USER_TYPE_ANDROID = 0;
	public static final int USER_TYPE_IPHONE = 1;
	//手机移动端广告
	public static final int USER_TYPE_MOBILE = 2;
	//大屏幕(PC/iPad)
	public static final int USER_TYPE_BIGSCREEN = 3;
	//其它设备
	public static final int USER_TYPE_OTHER = -1;
	//WIFI业务
	public static final int BUSINESS_WIFI = 0;
	//WAP幅窗业务
	public static final int BUSINESS_WAP = 1;
	//广电业务
	public static final int BUSINESS_GD = 2;
	
	public static final String TABLE_AD_INFO = "ad_info";
	public static final String TABLE_AD_INFO_URL = "ad_info_url";
	public static final String TABLE_AD_INFO_SEARCH = "ad_info_search";
	
	public static final int REPORT_STATUS_EXIST = 1;
	public static final int REPORT_STATUS_VIEW = 2;
	public static final int REPORT_STATUS_INSTALL = 3;

	/***********/
	// 广告信息
	public static final String AD_INFO = "adinfo";
	public static final String AD_CONTENT = "adcontent";
	// 广告列表
	public static final String AD_LIST = "adinfolist";
	// 广告显示类型
	public static final String AD_DISPLAY_TYPE = "addisplaytype";
	// AP切入点ID
	public static final String AP_UID = "apuid";
	//会话id
	public static final String SESSION_ID = "sid";
	//访问来源
	public static final String REFERRER = "referrer";
	//访问host
	public static final String HOST = "host";
	//用户IP
	public static final String IP = "ip";
	//客户端系统api版本号(andriod)
	public static final String API_INT = "api_int";
	// 广告名称
	public static final String AD_NAME = "adname";
	// 广告内容显示文件地址
	public static final String AD_DISPLAY_FILE_URL = "addisplayfileurl";
	// 广告文件下载地址
	public static final String AD_FILE_DOWNLOAD_URL = "adfiledownloadurl";
	//投放比例
	public static final String AD_RATIO = "ratio";
	public static final String AP_NODE_CODE = "nodecode";
	//广告投放网址
	public static final String AD_WEB_SITE_URL = "websiteurl";
	//广告投放位置
	public static final String AD_POSITION = "position";
	//插入广告代码
	public static final String AD_INSERT_CODE = "insertcode";
	//搜索字段
	public static final String AD_QUERY_FIELD = "queryfield";
	//搜索平台号
	public static final String AD_PF_CODE = "pfcode";
	//广告投放渠道号
	public static final String AD_PUSH_CHANNEL = "cid";
	//客户端访问来源url地址
	public static final String AD_PUSH_SRC_URL = "f";
	//规则
	public static String AD_RULE = "rule";
	//广告代码
	public static final String AD_CODE = "adcode";
	// 广告文件大小
	public static final String AD_FILE_SIZE = "adfilesize";
	// 广告展示的时间，单位为秒，如果长时间显示，返回最大整数
	public static final String AD_DISPLAY_TIME = "addisplaytime";
	//返回广告数
	public static final String RETURN_AD_COUNT = "returnadcount";
	// 广告检查的时间间隔
	public static final String AD_CHECK_INTERVAL = "adcheckinterval";// 用户请求广告
	public static final String INSTALL_MODE = "installmode";
	//橱窗广告桌面快捷键名称
	public static final String WINDOW_SHORTCUT_KEY_NAME = "shortcutkeyname";
	// 广告安装模式，1：直接下载广告文件，2：打开链接
	public static final int INSTALL_MODE_DOWNLOAD_FILE = 1;
	public static final int INSTALL_MODE_OPEN_URL = 2;

	//广告分类，普通投放
	public static final int AD_CATEGORY_STANDARD_PUSH = 1000;
	public static final int AD_CATEGORY_UNKNOWN_PUSH = 0;//未知
	// 上报步骤
	// 请求广告
	public static final int AD_REQUEST = 0;
	// 客户端的应用不允许显示广告
	public static final int AD_APP_NOT_ALLOW = -1;
	// 下发广告到客户端
	public static final int AD_PUSH_ARRIVE = 100;
	// 广告已被展示
	public static final int AD_PUSH_SHOW = 103;
	// 成功下载广告
	public static final int AD_PUSH_DOWNLOAD_SUCCESS = 104;
	// 下载广告失败
	public static final int AD_PUSH_DOWNLOAD_FAIL = 105;
	// 安装成功
	public static final int AD_PUSH_APK_INSTALL_COMPLETE = 106;
	// 加载资源失败
	public static final int AD_PUSH_RESOURCE_REQUIRED_PRELOAD_FAILED = 108;
	// 下载成功，用户点击显示安装页面
	public static final int AD_PUSH_CLICK_SHOW_INSTALL_VIEW = 110;
	// 用户点击了广告
	public static final int AD_PUSH_CLICK_DOWNLOAD_BUTTON = 115;
	// wifi网络
	public static final int NETWORK_TYPE_WIFI = 1;
	// 运营商网络
	public static final int NETWORK_TYPE_3G = 0;

	// 进入应用页面
	public static final int RPT_VIEW_DETAIL = 100;
	// 点击下载
	public static final int RPT_DOWNLOAD = 110;
	// 收藏
	public static final int RPT_FAVORITE = 120;

	// 模块ID
	public static final String MODEL_ID = "modelid";
	// 业务ID
	public static final String BUSINESS_ID = "busid";
	// 请求的页码
	public static final String PAGE_NUMBER = "pagenumber";
	// 列表
	public static final String LIST = "list";
	// 应用ID
	public static final String APP_ID = "appid";
	// 分类ID
	public static final String CAT_ID = "catid";
	public static final String CAT_IDS = "catids";
	public static final String PRIMARY_CAT_ID = "primarycatid";
	// 应用上报状态
	public static final String APP_STATUS = "appstatus";

	// 应用包名
	public static final String IDENTIFIER = "identifier";

	// 软件名称
	public static final String TITLE = "title";
	// 星级
	public static final String STAR_LEVEL = "startlevel";
	// 文件大小
	public static final String SIZE = "size";
	// 标签(0,1,2,3,4，5 无，首发，独家，新，荐，热）
	// 应用标志:无
	public static final int FLAG_NONE = 0;

	public static final String FLAG = "flag";
	// 应用标志:首发
	public static final String FLAG_FIRST_PUBLISH = "firstpublishflag";
	public static final int FLAG_FIRST_PUBLISH_VALUE = 1;
	// 应用标志:独家
	public static final String FLAG_EXCLUDE = "excludeflag";
	public static final int FLAG_EXCLUDE_VALUE = 2;
	// 应用标志:新产品
	public static final String FLAG_NEW = "newflag";
	public static final int FLAG_NEW_VALUE = 3;
	// 应用标志:推荐
	public static final String FLAG_RECOMMEND = "recommendflag";
	public static final int FLAG_RECOMMEND_VALUE = 4;
	// 应用标志:热门
	public static final String FLAG_HOT = "hotflag";
	public static final int FLAG_HOT_VALUE = 5;
	// 应用标志:飙升
	public static final String FLAG_RISE = "riseflag";
	public static final int FLAG_RISE_VALUE = 6;
	// 安装包下载地址
	public static final String FILE_DOWNLOAD_URL = "filedownloadurl";
	// ICON图片地址
	public static final String THUMB = "thumb";
	// 版本号
	public static final String VERSION = "version";
	// 游戏分类
	public static final String CAT_NAME = "catname";
	public static final String CAT_NAMES = "catnames";
	// 下载次数
	public static final String DOWNLOAD_TIMES = "downs";
	// 软件最近更新日期
	public static final String RELEASE_DATE = "updatedate";
	// 二级ID
	public static final String SECOND_LEVEL_ID = "secondlevelid";
	// 二级分类子类别
	public static final String SUB_TYPE = "subtype";
	// 简短描述
	public static final String SHORT_DESC = "shortdesc";
	// 软件内容简介
	public static final String CONTENT = "content";
	// 软件截图
	public static final String SCREENSHOT = "screenshot";
	// 1星
	public static final String RATING_1 = "rating1";
	// 2星
	public static final String RATING_2 = "rating2";
	// 3星
	public static final String RATING_3 = "rating3";
	// 4星
	public static final String RATING_4 = "rating4";
	// 5星
	public static final String RATING_5 = "rating5";

	public static final String RATING_ONE = "one";
	// 2星
	public static final String RATING_TWO = "two";
	// 3星
	public static final String RATING_THREE = "three";
	// 4星
	public static final String RATING_FOUR = "four";
	// 5星
	public static final String RATING_FIVE = "five";

	// 平均星级
	public static final String AVERAGE_RATING = "avgrating";
	// 用户名
	public static final String USERNAME = "username";
	// 星评级
	public static final String RATING = "rating";
	// 评论数
	public static final String RATING_COUNT = "ratingcount";
	// 日期
	public static final String DATE = "date";
	// 图片地址
	public static final String PIC_URL = "picurl";

	// 位置
	public static final String POS_ID = "posid";
	// 广告ID
	public static final String AD_ID = "adid";
	// 广告ID
	public static final String AD_IDS = "adids";

	public static final String KEYWORDS = "keywords";
	// 游戏分类列表
	public static final String GAME_CAT_LIST = "gamecatlist";
	// 软件分类列表
	public static final String SOFT_CAT_LIST = "softcatlist";

	// 上升最快列表
	public static final String RISE_RANKING_LIST = "riserankinglist";
	// 最新游戏
	public static final String NEW_RANKING_LIST = "newgameraingkinglist";
	// 应用排行
	public static final String SOFT_RANKING_LIST = "softrankinglist";
	// 游戏排行
	public static final String GAME_RANKING_LIST = "gamerankinglist";

	// 随心淘ID
	public static final String RANDOM_ID = "randomid";
	// 专题ID
	public static final String TOPIC_ID = "topicid";

	public static final String CODE = "code";

	// 客户端安装的所有应用
	public static final String INSTALLED_APP = "installedapp";

	public static final String NL_STORE_ID = "nlstoreid";
	public static final String APP_STORE_ID = "appstoreid";
	public static final String LAST_UPDATE_DATE = "lastupdatedate";
	public static final String STATUS = "status";
	public static final String TYPE_ID = "typeid";
	public static final String ID = "id";
	public static final String SEQ_NUM = "seqnum";
	public static final String START_DATE = "startdate";
	public static final String END_DATE = "enddate";
	
	public static final String UID = "uid";
	public static final String APP_NAME = "appname";
	public static final String ICON_URL = "iconurl";
	public static final String AD_SHAPE = "adshape";
	public static final String IS_DEL = "isdel";
	public static final String LAST_UPDATE_TIME = "lastupdatetime";
	public static final String CREATE_TIME = "createtime";
	public static final String IMG = "img";
	public static final String REMARK = "remark";
	public static final String DESCRIPTION = "description";
	public static final String TOTAL_PUSH_COUNT = "totalpushcount";
	
	public static final String ALLOW_PUSH = "allowpush";
	public static final String ALLOW_RICH_PUSH = "allowrichpush";
	public static final String ALLOW_LBS = "allowlbs";
	public static final String ALLOW_CPA = "allowcpa";
	public static final String ALLOW_CPS = "allowcps";
	public static final String CPS_VALUE = "cpsvalue";
	public static final String CPA_VALUE = "cpavalue";
	public static final String CPA_REBATE = "cparebate";
	public static final String PUSH_NUM = "pushnum";
	public static final String FIRST_PUSH_DELAY = "firstpushdelay";
	public static final String ENABLE_DELAY = "enabledelay";
	public static final String CPA_PERIOD = "cpaperiod";
	public static final String WEIGHT = "weight";
	public static final String APP_WHITE = "appwhite";
	public static final String AD_STINT = "adstint";
	public static final String ADDISPLAY_TYPE = "addisplaytype";
	public static final String APP_CHANNEL_ID = "appchannelid";
	public static final String CHANNEL_CODE = "channelcode";
	public static final String CHANNEL_UID = "channeluid";
	
	
	/*
	 * 0 推荐模块 1 游戏模块 2 专题列表 3 分类模块 4 随心淘模块 5 排行榜模块 6 软件详情模块
	 */
	// 推荐模块
	public static final int MODEL_RECOMMEND = 10;
	// 游戏模块
	public static final int MODEL_GAME = 20;
	// 专题列表
	public static final int MODEL_TOPIC = 30;
	// 二级专题列表
	public static final int MODEL_TOPIC_SECOND_LEVEL = 31;
	// 分类模块
	public static final int MODEL_CATEGORY = 40;
	// 二级分类模块
	public static final int MODEL_CATEGORY_SENCOND_LEVEL = 41;
	// 随心淘模块
	public static final int MODEL_RANDOM = 50;
	// 二级随心淘列表
	public static final int MODEL_RANDOM_SECOND_LEVEL = 51;
	// 排行榜模块
	public static final int MODEL_RANKING = 60;

	// 二级排行榜列表
	public static final int MODEL_RANKING_SECOND_LEVEL = 61;
	// 软件详情模块
	public static final int MODEL_DETAIL = 70;

	/*
	 * 0 推荐模块 — 广告推荐列表 1 推荐模块 — 软件列表 2 专题模块 — 专题列表 3 分类模块 — 分类列表 4 随心淘模块 — 随心淘列表
	 * 5 排行榜模块 — 排行榜列表 6 软件详细模块 — 软件详情 7 软件详细模块 — 推荐软件列表 8 软件详细模块 — 评论列表 9
	 * 软件详细模块 — 相关软件列表 10 专题模块 — 专题二级列表 11 分类模块 — 分类二级
	 */

	// 获取广告列表
	public static final int BUSINESS_AD_LIST = 1;
	// 获取应用列表
	public static final int BUSINESS_APP_LIST = 2;
	// 获取软件详情
	public static final int BUSINESS_APP_DETAIL = 3;
	// 获取推荐列表
	public static final int BUSINESS_RECOMMAND_LIST = 4;
	// 获取评论列表
	public static final int BUSINESS_COMMENT_LIST = 5;
	// 相关软件列表
	public static final int BUSINESS_RELATED_APP_LIST = 6;

	// 排行版上升最快
	public static final int RANKING_RISE = 1;
	// 排行版最新排行
	public static final int RANKING_NEW = 2;
	// 排行版软件排行
	public static final int RANKING_SOFT = 3;
	// 排行版游戏排行
	public static final int RANKING_GAME = 4;

	public static final int APP_CAT_SUB_TYPE_RASING = 1;
	public static final int APP_CAT_SUB_TYPE_NEW = 2;
	public static final int APP_CAT_SUB_TYPE_HOT = 3;

	public static final String LANG_EN = "EN";
	public static final String LANG_ZH = "ZH";

	public static final String DOMAIN_LANG = "LANG";

	public static final String DOMAIN_MODEL = "MODEL";
	//商品分类
	public static final String DOMAIN_GOODS_CATEGORY = "GOODS_CATEGORY";
	// 对话框广告
	public static final int AD_DISPLAY_TYPE_DIALOG = 1 << 1;
	public static final int AD_DISPLAY_TYPE_DIALOG_CODE = 1;
	// 插屏广告
	public static final int AD_DISPLAY_TYPE_INTERSTITIAL = 1 << 2;
	public static final int AD_DISPLAY_TYPE_INTERSTITIAL_CODE = 2;
	// 全屏广告
	public static final int AD_DISPLAY_TYPE_FULLSCREEN = 1 << 3;
	public static final int AD_DISPLAY_TYPE_FULLSCREEN_CODE = 3;
	// 头部banner条广告
	public static final int AD_DISPLAY_TYPE_BANNER = 1 << 4;
	public static final int AD_DISPLAY_TYPE_BANNER_CODE = 4;
	//底部广告条
	public static final int AD_DISPLAY_TYPE_BOTTOM_BANNER = 1 << 6;
	public static final int AD_DISPLAY_TYPE_BOTTOM_BANNER_CODE = 6;
	//PUSH推送广告
	public static final int AD_DISPLAY_TYPE_PUSH = 1 << 7;
	public static final int AD_DISPLAY_TYPE_PUSH_CODE = 7;
	
	/**
	 * 广告业务，10=URL替换
	 */
	public static final int AD_DISPLAY_TYPE_URL_REPLACE = 10;

	/**
	 * 广告业务，9=WIFI网页广告
	 */
	public static final int AD_DISPLAY_TYPE_WIFI = 9;
	
	/**
	 * 广告业务，8=组件广告
	 */
	public static final int AD_DISPLAY_TYPE_WIDGET = 8;
	/**
	 * 双边对联广告
	 */
	public static final int AD_DISPLAY_TYPE_COUPLET = 11;
	/**
	 * 浮窗广告
	 */
	public static final int AD_DISPLAY_TYPE_POPUP = 12;

	// 是否展示第三方广告，0展示自己的广告，1，展示地方广告
	public static final String DISPLAY_THIRD_PARTY_AD = "displaythirdpartyad";

	public static final String EXCLUDE_APP_WORD = "excludeappword";
	/*
	 * 客户端在拉取广告的时候会增加一个返回字段： “addisplaybycrossapp”:广告是否跨应用显示，
	 * 0，跨应用显示广告，1，不跨应用显示（这个字段暂时android专用）
	 * 当客户端收到的返回值是1的时候，将停止广告SDK的夸应用显示功能，只能在应用内显示广告。
	 */
	public static final String DISPLAY_BY_CROSSAPP = "addisplaybycrossapp";
	// 客户端定时器启动的时间间隔
	public static final String AD_TIMER_INTERVAL = "adtimerinterval";
	// 是否显示下载进度条
	public static final String DISPLAY_DOWN_PROGRESS = "displaydownprogress";

	// ios是否显示安装的提示
	public static final String DISPLAY_ALERT = "displayalert";

	// 是否激活安装的广告文件
	public static final String ACTIVE_APP = "activeapp";
	// 删除标记
	public static final String DELETE_FLAG = "Y";
	public static class Command {
		public static final int CMD_WHATTIME= 0; // 系统时间
		public static final int CMD_REGISTER = 1; // 注册
		public static final int CMD_AD_PSUH = 2;
		public static final int CMD_USER_STEP = 3;
		public static final int CMD_REPORT = 4;
		public static final int CMD_SERVER_CONFIG = 5;
		public static final int CMD_TEST_AD = 6;
		public static final int CMD_HEARTBEAT = 7;
		public static final int CMD_AD_PUSH_NOTIFICATION = 8;
		public static final int CMD_AD_ACTIVITY_CONTROL = 9;
		public static final int CMD_AD_WINDOW = 10;
	}

	/**
	 * android动态jar包的信息
	 */
	public static final String DYNAMIC_INFO = "dynamicinfo";
	public static final String TEMPLATE_VERSION = "dynamicinfo";

	/**
	 * 下载文件的时候是否限制网络类型，0，为不限制，1为限制 如果为0，不管什么时候都下载
	 * 如果为1，当用户在wifi下点击了下载，当网络切换到移动网络时，则暂停下载，其他情况则继续下载 这个参数可以根据文件的大小在代码中来限制
	 */
	public static final String LIMIT_NETWORK_TYPE = "limitnetworktype";
}

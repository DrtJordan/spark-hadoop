package com.jary.spark_hadoop.domain;

import java.util.Date;
import java.io.Serializable;

/**
 * 版权：成奕君兴 <br/>
 * 作者：hjl@haoyongapp.com <br/>
 * 生成日期：2016-06-21 <br/>
 * 描述：类
 */
public class Keyword implements Serializable {

	private static final long serialVersionUID = 1L;
    
    //自增长id
    private Integer id;
    
    //关键词
    private String keyword;
    
    //
    private Integer tagId;
    private String tagName;
    
    //
    private Date createTime;
    
    //状态:1：正常
    private Integer status;

    
    /**
     * 获得自增长id
     * @return Integer
     */
    public Integer getId(){
        return this.id;
    }

    /**
     * 设置自增长id
     * @param id  自增长id
     */
    public void setId(Integer id){
        this.id = id;
    }
    
    /**
     * 获得关键词
     * @return String
     */
    public String getKeyword(){
        return this.keyword;
    }

    /**
     * 设置关键词
     * @param keyword  关键词
     */
    public void setKeyword(String keyword){
        this.keyword = keyword;
    }
    
    /**
     * 获得
     * @return Integer
     */
    public Integer getTagId(){
        return this.tagId;
    }

    /**
     * 设置
     * @param tagId  
     */
    public void setTagId(Integer tagId){
        this.tagId = tagId;
    }
    
    /**
	 * @return the tagName
	 */
	public String getTagName() {
		return tagName;
	}

	/**
	 * @param tagName the tagName to set
	 */
	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	/**
     * 获得
     * @return Date
     */
    public Date getCreateTime(){
        return this.createTime;
    }

    /**
     * 设置
     * @param createTime  
     */
    public void setCreateTime(Date createTime){
        this.createTime = createTime;
    }
    
    /**
     * 获得状态:1：正常
     * @return Integer
     */
    public Integer getStatus(){
        return this.status;
    }

    /**
     * 设置状态:1：正常
     * @param status  状态:1：正常
     */
    public void setStatus(Integer status){
        this.status = status;
    }
}
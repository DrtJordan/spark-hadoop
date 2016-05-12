package com.jary.spark_hadoop.domain;

import java.util.Date;
import java.io.Serializable;

/**
 * 版权：成奕君兴 <br/>
 * 作者：hjl@haoyongapp.com <br/>
 * 生成日期：2016-06-15 <br/>
 * 描述：类
 */
public class TagMark implements Serializable {

	private static final long serialVersionUID = 1L;

    //打标签Id
    private Integer id;

    //
    private Integer tagId;

    //打标签对象（如：sid、adId、channel）
    private String relaPk;

    //关联类型：1：渠道；2：访客；3：广告
    private Integer relaType;

    //权重
    private Double weight;

    //
    private Date updateTime;

    //
    private Date createTime;

    //状态：1：正常；
    private Integer status;


    /**
     * 获得打标签Id
     * @return Integer
     */
    public Integer getId(){
        return this.id;
    }

    /**
     * 设置打标签Id
     * @param id  打标签Id
     */
    public void setId(Integer id){
        this.id = id;
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
     * 获得打标签对象（如：sid、adId、channel）
     * @return String
     */
    public String getRelaPk(){
        return this.relaPk;
    }

    /**
     * 设置打标签对象（如：sid、adId、channel）
     * @param relaPk  打标签对象（如：sid、adId、channel）
     */
    public void setRelaPk(String relaPk){
        this.relaPk = relaPk;
    }
    /**
     * 获得关联类型：1：渠道；2：访客；3：广告
     * @return Integer
     */
    public Integer getRelaType(){
        return this.relaType;
    }

    /**
     * 设置关联类型：1：渠道；2：访客；3：广告
     * @param relaType  关联类型：1：渠道；2：访客；3：广告
     */
    public void setRelaType(Integer relaType){
        this.relaType = relaType;
    }
    /**
     * 获得权重
     * @return Double
     */
    public Double getWeight(){
        return this.weight;
    }

    /**
     * 设置权重
     * @param weight  权重
     */
    public void setWeight(Double weight){
        this.weight = weight;
    }
    /**
     * 获得
     * @return Date
     */
    public Date getUpdateTime(){
        return this.updateTime;
    }

    /**
     * 设置
     * @param updateTime  
     */
    public void setUpdateTime(Date updateTime){
        this.updateTime = updateTime;
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
     * 获得状态：1：正常；
     * @return Integer
     */
    public Integer getStatus(){
        return this.status;
    }

    /**
     * 设置状态：1：正常；
     * @param status  状态：1：正常；
     */
    public void setStatus(Integer status){
        this.status = status;
    }
}
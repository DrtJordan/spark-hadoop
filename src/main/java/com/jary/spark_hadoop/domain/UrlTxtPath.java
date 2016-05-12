package com.jary.spark_hadoop.domain;

import java.util.Date;
import java.io.Serializable;

/**
 * 版权：成奕君兴 <br/>
 * 作者：hjl@haoyongapp.com <br/>
 * 生成日期：2016-08-31 <br/>
 * 描述：记录抓取url得到的文本txt的hdfs路径类
 */
public class UrlTxtPath implements Serializable {

	private static final long serialVersionUID = 1L;
    
    //
    private Integer id;
    
    //文件路径
    private String path;
    
    //创建时间
    private Date createTime;
    
    //更新时间
    private Date updateTime;
    
    //状态: 0：异常；1：待处理；2：处理中；3：待删除；4：完成
    private Integer status;

    
    /**
     * 获得
     * @return Integer
     */
    public Integer getId(){
        return this.id;
    }

    /**
     * 设置
     * @param id  
     */
    public void setId(Integer id){
        this.id = id;
    }
    
    /**
     * 获得文件路径
     * @return String
     */
    public String getPath(){
        return this.path;
    }

    /**
     * 设置文件路径
     * @param path  文件路径
     */
    public void setPath(String path){
        this.path = path;
    }
    
    /**
     * 获得创建时间
     * @return Date
     */
    public Date getCreateTime(){
        return this.createTime;
    }

    /**
     * 设置创建时间
     * @param createTime  创建时间
     */
    public void setCreateTime(Date createTime){
        this.createTime = createTime;
    }
    
    /**
     * 获得更新时间
     * @return Date
     */
    public Date getUpdateTime(){
        return this.updateTime;
    }

    /**
     * 设置更新时间
     * @param updateTime  更新时间
     */
    public void setUpdateTime(Date updateTime){
        this.updateTime = updateTime;
    }
    
    /**
     * 获得状态: 0：异常；1：待处理；2：处理中；3：待删除；4：完成
     * @return Integer
     */
    public Integer getStatus(){
        return this.status;
    }

    /**
     * 设置状态: 0：异常；1：待处理；2：处理中；3：待删除；4：完成
     * @param status  状态: 0：异常；1：待处理；2：处理中；3：待删除；4：完成
     */
    public void setStatus(Integer status){
        this.status = status;
    }
}
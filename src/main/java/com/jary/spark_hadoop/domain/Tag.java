package com.jary.spark_hadoop.domain;

import java.util.Date;
import java.io.Serializable;

/**
 * 版权：成奕君兴 <br/>
 * 作者：hjl@haoyongapp.com <br/>
 * 生成日期：2016-06-15 <br/>
 * 描述：类
 */
public class Tag implements Serializable {

	private static final long serialVersionUID = 1L;

    //标签Id
    private Integer id;

    //标签名
    private String name;

    //标签类型:0：未分类
    private Integer type;

    //父类标签Id
    private Integer parent;

    //是否叶子节点：1：是；0：否
    private Integer isLeaf;

    //
    private Date createTime;

    //状态：1：正常；2：失效
    private Integer status;


    /**
     * 获得标签Id
     * @return Integer
     */
    public Integer getId(){
        return this.id;
    }

    /**
     * 设置标签Id
     * @param id  标签Id
     */
    public void setId(Integer id){
        this.id = id;
    }
    /**
     * 获得标签名
     * @return String
     */
    public String getName(){
        return this.name;
    }

    /**
     * 设置标签名
     * @param name  标签名
     */
    public void setName(String name){
        this.name = name;
    }
    /**
     * 获得标签类型:0：未分类
     * @return Integer
     */
    public Integer getType(){
        return this.type;
    }

    /**
     * 设置标签类型:0：未分类
     * @param type  标签类型:0：未分类
     */
    public void setType(Integer type){
        this.type = type;
    }
    /**
     * 获得父类标签Id
     * @return Integer
     */
    public Integer getParent(){
        return this.parent;
    }

    /**
     * 设置父类标签Id
     * @param parent  父类标签Id
     */
    public void setParent(Integer parent){
        this.parent = parent;
    }
    /**
     * 获得是否叶子节点：1：是；0：否
     * @return Integer
     */
    public Integer getIsLeaf(){
        return this.isLeaf;
    }

    /**
     * 设置是否叶子节点：1：是；0：否
     * @param isLeaf  是否叶子节点：1：是；0：否
     */
    public void setIsLeaf(Integer isLeaf){
        this.isLeaf = isLeaf;
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
     * 获得状态：1：正常；2：失效
     * @return Integer
     */
    public Integer getStatus(){
        return this.status;
    }

    /**
     * 设置状态：1：正常；2：失效
     * @param status  状态：1：正常；2：失效
     */
    public void setStatus(Integer status){
        this.status = status;
    }
}
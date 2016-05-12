package com.jary.spark_hadoop.domain;

import java.io.Serializable;

/**
 * 版权：成奕君兴 <br/>
 * 作者：hjl@haoyongapp.com <br/>
 * 生成日期：2016-08-30 <br/>
 * 描述：类
 */
public class Category implements Serializable {

	private static final long serialVersionUID = 1L;
    
    //
    private Integer id;
    
    //应用类型名称
    private String typeName;
    
    //描述
    private String description;
    
    //上一级分类
    private Integer parent;
    
    //是否可见，1=可见，0=隐藏 
    private Integer visible;
    
    //
    private Integer orderNo;
    
    //扩展json字符串
    private String extensions;

    
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
     * 获得应用类型名称
     * @return String
     */
    public String getTypeName(){
        return this.typeName;
    }

    /**
     * 设置应用类型名称
     * @param typeName  应用类型名称
     */
    public void setTypeName(String typeName){
        this.typeName = typeName;
    }
    
    /**
     * 获得描述
     * @return String
     */
    public String getDescription(){
        return this.description;
    }

    /**
     * 设置描述
     * @param description  描述
     */
    public void setDescription(String description){
        this.description = description;
    }
    
    /**
     * 获得上一级分类
     * @return Integer
     */
    public Integer getParent(){
        return this.parent;
    }

    /**
     * 设置上一级分类
     * @param parent  上一级分类
     */
    public void setParent(Integer parent){
        this.parent = parent;
    }
    
    /**
     * 获得是否可见，1=可见，0=隐藏 
     * @return Integer
     */
    public Integer getVisible(){
        return this.visible;
    }

    /**
     * 设置是否可见，1=可见，0=隐藏 
     * @param visible  是否可见，1=可见，0=隐藏 
     */
    public void setVisible(Integer visible){
        this.visible = visible;
    }
    
    /**
     * 获得
     * @return Integer
     */
    public Integer getOrderNo(){
        return this.orderNo;
    }

    /**
     * 设置
     * @param orderNo  
     */
    public void setOrderNo(Integer orderNo){
        this.orderNo = orderNo;
    }
    
    /**
     * 获得扩展json字符串
     * @return String
     */
    public String getExtensions(){
        return this.extensions;
    }

    /**
     * 设置扩展json字符串
     * @param extensions  扩展json字符串
     */
    public void setExtensions(String extensions){
        this.extensions = extensions;
    }
}
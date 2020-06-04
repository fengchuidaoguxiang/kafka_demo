package com.baizhi.transform.jdk;

import java.io.Serializable;
import java.util.Date;

/**
 * 自定义对象 必须实现序列化接口
 */
public class User implements Serializable {

    private Integer id;

    private String name;

    private Boolean sex;

    // 不参与序列化
    transient private Date birthday;

    public User() {
    }

    public User(Integer id, String name, Boolean sex, Date birthday) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.birthday = birthday;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }


    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex=" + sex +
                ", birthday=" + birthday +
                '}';
    }
}

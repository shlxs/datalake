package cn.sunrisecolors.datalake.msgpack;

import lombok.Data;

/**
 * @author shaohongliang
 * @since 2019/9/9 15:05
 */
@Data
public class ExamplePojo {

    private transient String name;

    private int age;

    private String sex;

    public ExamplePojo(){

    }

    public ExamplePojo(String name){
        this.name = name;
    }
}

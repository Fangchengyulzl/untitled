package com.itheim.flumedemo.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.HostInterceptor;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 我的host拦截器
 */

public class MyHostInterceptor implements Interceptor {

    private String name;

    //使用日志
    private static final Logger logger = LoggerFactory.getLogger(MyHostInterceptor.class);

    //初始化操作,设置默认值
    @Override
    public void initialize() {
        this.name = "";
    }

    //事件拦截器
    @Override
    public Event intercept(Event event) {
        //对事件进行处理。事件包含消息体和头部
        //如果host来源是192.168.200.130，对事件做一个抛弃处理
        if (event.getHeaders().get("host").equals("192.168.200.130")){
            //将结果打印到控制台
            logger.info("消息来源是130，抛弃事件");
            return null;
        }
        //如果满足某个条件
        Map<String,String> map = new HashMap<String,String>();
        map.put("state","CZ");
        event.setHeaders(map);
        return event;
    }

    //处理所有事件
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new ArrayList<Event>();
        //遍历
        for (Event event : list) {
            //调用上面的对事件单个仅从处理的event
            Event event1 = intercept(event);
            //判断，如果在单条事件处理中为空，则不返回值
           if (event1!=null){
               eventList.add(event1);
           }
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new MyHostInterceptor();
        }

        //初始化的配置
        @Override
        public void configure(Context context) {

        }
    }
}

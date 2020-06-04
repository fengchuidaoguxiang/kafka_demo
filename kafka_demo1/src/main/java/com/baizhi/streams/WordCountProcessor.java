package com.baizhi.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.HashMap;

/**
 *  单词计数的处理器
 */
public class WordCountProcessor implements Processor<String, String> {

    // 自定义对象 状态数据存储的
    // private HashMap<String, Long> mapStates = new HashMap<String, Long>();

    // kafka streams提供状态管理
    private KeyValueStore<String , Long> keyValueStore = null;

    private ProcessorContext context = null;



    /**
     * 初始化方法
     * @param processorContext  处理器的上下文对象    包含了程序运行环境信息    需要context将数据发送到下游的处理器
     */
    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
        /*
        // 定期将处理器的处理结果发送给下游的处理器
        processorContext.schedule(
                Duration.ofSeconds(1),
                PunctuationType.STREAM_TIME,
                new Punctuator() {
                    @Override
                    public void punctuate(long ts) {
                        System.out.println("-------------" + ts + "---------------");
                        mapStates.forEach((k, v) -> {
                            // 转发
                            processorContext.forward(k, v);
                        });
                    }
                 }
        );
         */

        // 通过历史状态数据 恢复还原状态数据
        keyValueStore = (KeyValueStore<String, Long>) context.getStateStore("Counts");
        processorContext.schedule(
                Duration.ofSeconds(1),
                PunctuationType.STREAM_TIME,
                new Punctuator() {
                    @Override
                    public void punctuate(long ts) {
                        System.out.println("-------------" + ts + "---------------");
                        KeyValueIterator<String, Long> iterator = keyValueStore.all();
                        while (iterator.hasNext()){
                            KeyValue<String, Long> keyValue = iterator.next();
                            String word = keyValue.key;
                            Long num = keyValue.value;
                            context.forward(word,  num);
                        }
                    }
                }
        );

    }

    /**
     * 处理方法
     * @param k
     * @param v    Hello  Kafka  |  Hello   Hadoop
     */
    @Override
    public void process(String k, String v) {
        /*
        if( v != null && v.length() != 0 ){
            String[] words = v.split("\\s");
            for (String word : words) {
                // 获取真实值 或者赋予默认值
                Long num = mapStates.getOrDefault(word, 0L);
                num ++;
                mapStates.put(word, num);
            }
        }
         */
        if( v != null && v.length() != 0 ){
            String[] words = v.split(" ");
            for (String word : words) {
                // 获取真实值 或者赋予默认值
                Long num = keyValueStore.get(word);
                if( num == null ){
                    keyValueStore.put(word, 1L);
                }else {
                    keyValueStore.put(word,num + 1L);
                }
            }
        }

        // 确认流数据处理完毕
        context.commit();
    }

    /**
     * 关闭方法
     */
    @Override
    public void close() {

    }
}

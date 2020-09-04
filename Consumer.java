package cn.liadrinz.monkeydoc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

class MonkeyConsumer<K,V>{

    private KafkaConsumer<K,V> consumer;
    private String topic = "";

    public MonkeyConsumer(String address, String groupId, String topic, Deserializer<K> keyDeSerializer, Deserializer<V> valueDeSerializer)
    {
        Properties properties = new Properties();
        //连接kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,address);
        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置 offset自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交间隔时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer.getClass().getName());
        //value反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer.getClass().getName());

        this.topic = topic;

        consumer = new KafkaConsumer<K,V>(properties);
    }

    public void receiveMessage()
    {
        while(true) {
            List<String> result = new ArrayList<String>();
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<K, V> record : records) {
                String str = record.value().toString();
                result.add(str);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        MonkeyConsumer<String,String> consumer = new MonkeyConsumer<String,String>("121.36.15.90:9092","MonkeyDoc","test",new StringDeserializer(),new StringDeserializer());
        
    }


}
































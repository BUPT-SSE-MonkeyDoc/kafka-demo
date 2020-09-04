package cn.liadrinz.monkeydoc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class MonkeyProducer<K,V>{
    private KafkaProducer<K,V> producer;

    public MonkeyProducer(String address, Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,address);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
        producer = new KafkaProducer<K, V>(properties);
    }

    public void sendMessage(K key, V value, String topic) throws ExecutionException, InterruptedException {
        this.producer.send(new ProducerRecord<K, V>(topic,key,value)).get();
    }

    public void sendMessage(V value,String topic) throws ExecutionException, InterruptedException {
        this.producer.send(new ProducerRecord<K, V>(topic,value)).get();
    }

    public void closeConnection()
    {
        this.producer.close();
    }

  public static void main(String[] args) {
//        Properties properties = new Properties();
//        //连接kafka集群
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"121.36.15.90:9092");
//        //key序列化
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        //value序列化
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        //创建生产者
//        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

      MonkeyProducer<String,String> producer = new MonkeyProducer<String, String>("121.36.15.90:9092",new StringSerializer(),new StringSerializer());
        int num = 0;
        while(num < 10){
            String msg = "kafka test" + num;
            try {
//                producer.send(new ProducerRecord<String, String>("test",String.valueOf(num),msg)).get();
                producer.sendMessage(String.valueOf(num),msg,"test");
                TimeUnit.SECONDS.sleep(2);
                num++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
      producer.closeConnection();
    }
}




























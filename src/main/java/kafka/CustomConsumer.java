package kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AutoSendEmail;

import javax.mail.MessagingException;

public class CustomConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CustomConsumer.class);

    @SuppressWarnings("resource")
    public static void main(String[] args) throws MessagingException {

        //1 配置消费者属性
        Properties pros = new Properties();
        // 配置Kafka集群的地址
        pros.put("bootstrap.servers", "yun:9092");
        // 设置消费者
        pros.put("group.id", "g1");
        // 是否自动确认offset，默认就是true，不设置也可以。
        pros.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔  ， 每隔1秒确认一下offset是否正确。
        pros.put("auto.commit.interval.ms", "1000");
        // key的反序列化
        pros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列化
        pros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2 创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);

        //4 释放资源
        //在jvm将要终止的时候，增加一个Shutdown线程，该线程完成收尾工作。
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            public void run() {
                if(consumer!=null){
                    System.out.println("释放资源了");
                    consumer.close();
                }
            }
        }));

        //3 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("linux_login"));

        //3 拉消息
        while(true){
            //读取数据，每隔100ms读取一次数据.生产者发消息时将消息封装成ProducerRecord<K,V>，那么消费者拉消息时将消息封装成ConsumerRecords<K,V>
            //ConsumerRecords<String, String>相当于是一个map集合。
            ConsumerRecords<String, String> records = consumer.poll(100);
            //我们一再强调消费者消费数据时，是按照时间窗口来消费的，并不是一条一条读取的，而是每隔多少时间消费者一次性拉来多少数据，可能是10条，也可能是20条。
            //这里将最大的时间间隔设置为100毫秒，也就说消费者最大每100毫秒拉一次消息。
            //如果写入数据时没有指定key的话，那么取出的key是null
            for (ConsumerRecord<String, String> record  : records) {
                String value = record.value();

                if(value.contains("Accepted publickey for")){
                    LOG.info(value);
                    AutoSendEmail.config(AutoSendEmail.SMTP_163(false), "@163.com", "");
                    AutoSendEmail.subject("百度云登录")
                            .from("@163.com")
                            .to("1394245247@qq.com")
                            .text(value)
                            .send();
                }
            }
        }




    }
}

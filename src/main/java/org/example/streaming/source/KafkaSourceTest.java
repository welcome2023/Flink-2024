package org.example.streaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 17:23
 * @usage
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(2);
        // 构造KafkaSource   使用建造者模式构造
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("cdh01:9092")  //如何连接集群
                .setGroupId("test001") //设置组名
                .setTopics("test1116")   //设置读取哪个主题
        // 策略不写，默认就是latest
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))  //设置从哪个位置读取
                /*
                    生产：  数据---->ProducerRecord ---->序列化为 byte[] ----->broker-->存储到文件
                    消费:   broker-->存储到文件---->byte[] ----->反序列化为 ConsumerRecord
                    ProducerRecord和ConsumerRecord都是KEY-VALUE结构。key只有一个作用，就是在生产的时候分区。
                        相同的key的数据会自动放入同一个partition.
                        数据都是在value中，因此消费者消费时，只需要消费value!
                        .setValueOnlyDeserializer(): 只消费value，只需要设置value的反序列化器。 99%
                        .setDeserializer(): 消费key和value.设置key和value如何反序列化。
                 */
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //设置其他的消费者参数
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
                .build();
        //把source添加到环境中
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkaSource")
                .print();
       env.execute();
    }
}

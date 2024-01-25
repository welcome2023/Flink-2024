package org.example.streaming.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.function.WaterSensorMapFunction;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 18:15
 * @usage
 */
public class KafkaSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //新的sinkAPI，需要开启ck
        //env.enableCheckpointing(5000);
        //准备KafkaSink  角色：生产者
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers("cdh01:9092")
                //设置序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("test1116") //写到哪个主题
                                .setValueSerializationSchema(new SimpleStringSchema()) //value的反序列化器必须要设置
                                .build()
                        //.setPartitioner() //指定自定义的分区器，不指定，默认
                )
                //其他的生产者设置
                //.setProperty()
                //语义保证  EOS需要开启CK
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //设置kafka生产者事务id的前缀，可以在broker中的_transaction_state中查看。EOS，必须要设置
                .setTransactionalIdPrefix("cms-")
                .build();
        env
                .socketTextStream("cdh01", 8888)
                .map(new WaterSensorMapFunction())
                .map(JSON::toJSONString)
                .sinkTo(kafkaSink);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package org.example.streaming.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.example.bean.WaterSensor;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 17:35
 * @usage
 */
public class DataGenTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataGeneratorSource<WaterSensor> dataGeneratorSource = new DataGeneratorSource<WaterSensor>(new RandomGenerator<WaterSensor>() {
            //       构造数据 模拟id:s1---s10,    currentTime,  vc:1000-3000
            @Override
            public WaterSensor next() {
                return new WaterSensor(
                        "s" + RandomUtils.nextInt(1, 11),
                        System.currentTimeMillis(),
                        RandomUtils.nextInt(1000, 3001)
                );
            }
        },              // 生成数据
                100,      // 每秒生成多少条,总行数。
                1000L);  // 一共要生成多少条，如果是无限，传null。不是null，就是有界流

        // 把数据源加入到环境,是一个可以并行运行的Source
        env.addSource(dataGeneratorSource)
                .returns(WaterSensor.class)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

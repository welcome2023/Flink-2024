package org.example.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.bean.WaterSensor;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 18:17
 * @usage
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String line) throws Exception {
        String[] words = line.split(",");
        return new WaterSensor(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
    }
}

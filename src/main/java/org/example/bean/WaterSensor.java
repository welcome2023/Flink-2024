package org.example.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cmsxyz@163.com
 * @date 2024/1/25 17:38
 * @usage
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    @Override
    public String toString() {
        return "ws("+id+","+ts+","+vc+")";
    }
}

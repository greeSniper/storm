package com.gaiaworks.entity;

import com.gaiaworks.utils.DateUtils;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by 唐哲
 * 2018-02-22 9:03
 */
@Data
public class Punch implements Serializable {

    private String personId;
    private String name;
    private Long punchTime;
    private Long workStartTime;
    private Long noonStartTime;
    private Long noonEndTime;
    private Long workEndTime;

    //0:上班打卡, 1:午休前打卡, 2:午休后打卡, 3:下班打卡
    private Integer punchFlag;

    //打卡情况
    private String msg = "正常";

    private Long costTime;

    public Punch() {}

    public Punch(String personId, String name, Long punchTime,
                 Long workStartTime, Long noonStartTime,
                 Long noonEndTime, Long workEndTime) {
        this.personId = personId;
        this.name = name;
        this.punchTime = punchTime;
        this.workStartTime = workStartTime;
        this.noonStartTime = noonStartTime;
        this.noonEndTime = noonEndTime;
        this.workEndTime = workEndTime;
    }

    public String toString() {
        return "Punch{" +
                "personId='" + personId + '\'' +
                ", name='" + name + '\'' +
                ", punchTime=" + DateUtils.getInstance().getString(punchTime) +
                ", punchFlag=" + punchFlag +
                ", msg='" + msg + '\'' +
                ", costTime=" + costTime +
                '}';
    }

}

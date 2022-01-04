package org.flink.hackathon.model;

import lombok.Data;

/**
 * @author chenrui.a@mininglamp.com
 * @project flink-hackathon
 * @date 2022/01/04
 */
@Data
public class BussMsg {
    private Integer customId;
    private Integer institutionId;
    private Integer businessType;
    private Float businessAmount;
    private Long createTime;
    private String createTimeStr;
    private Long appointmentTime;
    private String appointmentTimeStr;
    private Integer managerId;
    private Integer company;
}

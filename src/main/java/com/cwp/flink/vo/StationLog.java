package com.cwp.flink.vo;

import lombok.Data;

@Data
public class StationLog {



    private String stationId;

    private Integer  index;

    private String callOut;

    private String  callIn;

    private Long  time;

    private String type;

    private Long duration;






}

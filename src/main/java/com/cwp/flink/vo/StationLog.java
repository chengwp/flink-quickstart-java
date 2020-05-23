package com.cwp.flink.vo;


public class StationLog {

    private Integer  index;

    private String callOut;

    private String  callIn;

    private Long  time;

    private String type;


    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getCallOut() {
        return callOut;
    }

    public void setCallOut(String callOut) {
        this.callOut = callOut;
    }

    public String getCallIn() {
        return callIn;
    }

    public void setCallIn(String callIn) {
        this.callIn = callIn;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "StationLog{" +
                "index=" + index +
                ", callOut='" + callOut + '\'' +
                ", callIn='" + callIn + '\'' +
                ", time=" + time +
                ", type='" + type + '\'' +
                '}';
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }



}

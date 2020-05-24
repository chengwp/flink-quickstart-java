package com.cwp.flink.time;

import com.cwp.flink.vo.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class TestWaterMark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.getConfig().setAutoWatermarkInterval(100L);
        DataStream dataStream=streamEnv.socketTextStream("192.168.10.198",9000);
        dataStream.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String s) throws Exception {
                String[] strArr=s.split(",");
                StationLog stationLog=new StationLog();
                stationLog.setIndex(Integer.valueOf(strArr[0]));
                stationLog.setCallOut(strArr[1]);
                stationLog.setCallIn(strArr[2]);
                stationLog.setStationId(strArr[3]);
                stationLog.setTime(Long.valueOf(strArr[4]));
                stationLog.setType(strArr[5]);
                stationLog.setDuration(Long.valueOf(strArr[6]));
                return  stationLog;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StationLog>() {

            long maxTime=0;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTime-3000L);
            }

            @Override
            public long extractTimestamp(StationLog element, long previousElementTimestamp) {
                 if(element.getTime().compareTo(maxTime)>0){
                    maxTime=element.getTime();
                 }
                 return element.getTime();
            }
        })/*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(StationLog element) {
                return element.getTime();
            }
        })*/.filter(new FilterFunction<StationLog>() {
            @Override
            public boolean filter(StationLog value) throws Exception {
                 if(value.getType().equals("success")){
                     return true;
                 }else{
                     return false;
                 }
            }
        }).keyBy(new KeySelector<StationLog,String>() {

            @Override
            public String getKey(StationLog value) throws Exception {
                return value.getStationId();
            }
        }).timeWindow(Time.seconds(10),Time.seconds(5))
                .reduce(new ReduceFunction<StationLog>() {
                    @Override
                    public StationLog reduce(StationLog value1, StationLog value2) throws Exception {
                        if (value1.getDuration() > value2.getDuration()) {
                            return value1;
                        } else {
                            return value2;
                        }
                    }
                },new WindowFunction<StationLog,String,String,TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow window, Iterable<StationLog> input, Collector<String> out) throws Exception {
                        StationLog sl=input.iterator().next();
                        StringBuffer sb=new StringBuffer("");
                        sb.append("窗口范围【 开始时间："+window.getStart()+" 结束时间："+window.getEnd()+"】").append("\n");
                        sb.append("ID："+sl.getStationId()+"callOut:"+sl.getCallOut()+",callIn:"+sl.getCallIn()+",duration:"+sl.getDuration()
                        +"time:"+sl.getTime()+"\n");
                         out.collect(sb.toString());

                    }
                })
                .print();


        streamEnv.execute();




    }
}

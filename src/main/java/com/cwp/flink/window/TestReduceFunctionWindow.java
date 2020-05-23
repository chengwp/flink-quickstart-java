package com.cwp.flink.window;


import com.cwp.flink.vo.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TestReduceFunctionWindow {


    public static void main(String[] args) {
        StreamExecutionEnvironment   streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream   dataStream=streamEnv.socketTextStream("192.168.10.198",9000);
        dataStream.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String s) throws Exception {
                String[] strArr=s.split(",");
                StationLog stationLog=new StationLog();
                stationLog.setIndex(Integer.valueOf(strArr[0]));
                stationLog.setCallOut(strArr[1]);
                stationLog.setCallIn(strArr[2]);
                stationLog.setTime(Long.valueOf(strArr[3]));
                stationLog.setType(strArr[4]);
                return  stationLog;
            }
        }).map(new MapFunction<StationLog, Tuple2<String,Integer>>(){
            @Override
            public Tuple2<String, Integer> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.getType(),1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String,Integer>>() {

                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                        return Tuple2.of(t0.f0,t0.f1+t1.f1);
                    }
                }).print();


        try {
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

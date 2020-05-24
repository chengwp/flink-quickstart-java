package com.cwp.flink.window;


import com.cwp.flink.vo.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每隔10秒，统计成功与失败的日志数量
 */
public class TestProcessFunctionWindow {


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
        }).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //一个窗口结束的时候调用一次,整个窗口的数据保存在Iterable中，里面有很多行数据,Iterable中的size就是日志的总数
                .process(new ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Long>,Tuple1<String>,TimeWindow>() {

                    @Override
                    public void process(Tuple1<String> s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        System.out.print("time:"+System.currentTimeMillis());
                        out.collect(Tuple2.of(s.f0,elements.spliterator().estimateSize()));
                    }
                }).print();


        try {
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

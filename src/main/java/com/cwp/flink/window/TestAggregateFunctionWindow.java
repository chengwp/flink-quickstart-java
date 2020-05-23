package com.cwp.flink.window;


import com.cwp.flink.vo.StationLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class TestAggregateFunctionWindow {


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
        }).keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String,Integer>,Integer,Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 来一条数据执行一次
                     * @param stringIntegerTuple2
                     * @param integer
                     * @return
                     */
                    @Override
                    public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
                        return integer+stringIntegerTuple2.f1;
                    }

                    /**
                     * 窗口结束的时候执行一次
                     * @param integer
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer+acc1;
                    }
                    // WindowFuction的值来源于AggregateFunction
                }, new WindowFunction<Integer, Tuple2<String, Integer>, Tuple1<String>, TimeWindow>() {
                    /**
                     * 在窗口结束的时候先执行AggregateFunction中的getResult,然后再执行apply
                     * @param s
                     * @param timeWindow
                     * @param iterable
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple1<String> s, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(Tuple2.of(s.f0,iterable.iterator().next()));
                    }
                }).print();

        try {
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

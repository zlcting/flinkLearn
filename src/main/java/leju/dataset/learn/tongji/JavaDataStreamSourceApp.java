package leju.dataset.learn.tongji;
import leju.dataset.learn.course05.JavaCustomParallelSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JavaDataStreamSourceApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        nonParallelSourceFunction(env);
        env.execute("JavaDataStreamSourceApp");
    }

    public static void nonParallelSourceFunction(StreamExecutionEnvironment env){
        DataStream<String> data = env.addSource(new SourceFunction());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //data.print();
        DataStream<Tuple6<String,String,String,String,String,String>> inputMap =  data.map(new MapFunction<String, Tuple6<String,String,String,String,String,String>>() {
                    @Override
                    public Tuple6<String,String,String,String,String,String> map(String s) throws Exception {
                        String[] val = s.split("\t");
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = simpleDateFormat.parse(val[2]);
                        String ts = String.valueOf( date.getTime());

                        return  new Tuple6<String,String,String,String,String,String>(val[0],val[1],ts,val[3],val[4],val[5]);
                    }
                });

        DataStream<Tuple6<String,String,String,String,String,String>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple6<String, String, String, String, String, String>>() {
            @Nullable
            private final long maxOutOfOrderness = 3500; // 3.5 seconds

            private long currentMaxTimestamp;
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple6<String, String, String, String, String, String> element, long previousElementTimestamp) {
                Long timestamp = Long.parseLong(element.f2);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;

            }
        });

        waterMarkStream.print().setParallelism(1);

    }

    //参考
    //https://blog.csdn.net/xu470438000/article/details/83271123

}

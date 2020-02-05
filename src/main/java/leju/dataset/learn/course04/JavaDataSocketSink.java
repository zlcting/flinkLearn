package leju.dataset.learn.course04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataSocketSink {

    public static void main(String[] args) throws Exception{
        //writeToSocket
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9001, "\n");

        text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "re:"+value;
            }
        }).print();
        //text.writeToSocket("127.0.0.1", 9001,new SimpleStringSchema());

        env.execute("JavaDataSocketSink");
    }
}

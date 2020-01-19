package leju.dataset.learn.course08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;


public class JavaHDFSApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1",9001);
        text.print();
        text.addSink(new BucketingSink<String>("file:///home/zlc/project/flink_project/learn/input"));
        env.execute("Flink Streaming Java API Skeleton");
    }
}

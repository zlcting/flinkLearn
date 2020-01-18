package leju.dataset.learn.course08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class JavakafkaApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.208.86.17:9092");
        properties.setProperty("zookeeper.connect", "10.208.86.17:2181");
        properties.setProperty("group.id", "grouptest");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("test", new SimpleStringSchema(), properties));
        stream.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}

package leju.dataset.learn.course08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class JavatokafkaApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("127.0.0.1",9001);

        // 2.0 配置 kafkaProducer
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "10.208.86.17:9092",            // broker list
                "test",                  // target topic
                new SimpleStringSchema());   // serialization schema


        myProducer.setWriteTimestampToKafka(true);
        // 2.1 把kafka设置为sink
        stream.addSink(myProducer);
        stream.print();

        env.execute("JavatokafkaApp");

    }


}

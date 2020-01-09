package leju.dataset.learn.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class DataSetSinkApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<Integer>();
        for (int i =1;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        String filePaht = "/home/zlc/project/flink_project/learn/input/sink_out";
        data.writeAsText(filePaht, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        env.execute("DataSetSinkApp");
    }
}

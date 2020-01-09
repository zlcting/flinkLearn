package leju.dataset.learn.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JavaDistributedCacheApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePaht = "file:///home/zlc/project/flink_project/learn/input/hello.txt";
        //注册一个本地/hdfs文件
        env.registerCachedFile(filePaht,"pl-java-dc");
        DataSource<String> data = env.fromElements("haoop","linux","flink","php","golang","python");

        data.map(new RichMapFunction<String, String>() {
            List<String> list = new ArrayList<String>();
            @Override
            public void open(Configuration parameters) throws Exception{
               File file =  getRuntimeContext().getDistributedCache().getFile("pl-java-dc");
               List<String> lines = FileUtils.readLines(file);
               for (String line : lines){
                    list.add(line);
                   System.out.println("line = ["+line+"]");
               }
            }

            public String map(String value) throws Exception {
                return value;
            }
        }).print();
    }
}

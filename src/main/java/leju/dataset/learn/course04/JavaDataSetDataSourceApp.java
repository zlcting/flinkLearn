package leju.dataset.learn.course04;


import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //fromCollection(env);
        textFile(env);
    }

    public static void textFile(ExecutionEnvironment env)throws Exception{
        String filePath = "file:///home/zlc/project/flink_project/learn/input/hello.txt";
        env.readTextFile(filePath).print();
        System.out.println("~~~~~~~~~~分割线~~~~~~~~~~~~~~~~");
        filePath = "file:///home/zlc/project/flink_project/learn/input";
        env.readTextFile(filePath).print();
    }


    public static void fromCollection(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for (int i=1;i<=10;i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}

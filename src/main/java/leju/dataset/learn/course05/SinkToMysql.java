package leju.dataset.learn.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMysql extends RichSinkFunction<Student> {
    Connection connection;
    PreparedStatement pstmt;

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://127.0.0.1:3306/bishe";
            conn = DriverManager.getConnection(url,"root","123");
        } catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public void open(Configuration parameters)throws Exception{
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student (id,name,age) values (?,?,?)";
        pstmt = connection.prepareStatement(sql);
    }

    //每条记录插入时调用一次
    public void invoke(Student value,Context context) throws Exception{
        //为前面的占位符赋值
        pstmt.setInt(1,value.getId());
        pstmt.setString(2,value.getName());
        pstmt.setInt(3,value.getAge());

        pstmt.executeUpdate();
    }

    public void close() throws Exception{
        super.close();

        if(pstmt !=null){
            pstmt.close();
        }

        if (connection !=null){
            connection.close();
        }
    }

}

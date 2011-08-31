package com.panelion.utils.db;

import javax.sound.midi.VoiceStatus;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 22.
 * Time: 오후 3:17
 *
 * Oracle Connection 을 관리 하는 클래스.
 * 이 연결을 올바로 사용 하기 위해 서는 Oracle jdbc Driver 가 필요 하다.
 *
 */
public class OracleConnector {

    private static Connection conn = null;
    public static Connection getConnection(String url, String user, String pwd) {
        if(conn == null) {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                conn = DriverManager.getConnection(url, user, pwd);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return conn;
    }

    public static void close() {
        if(conn != null) {
            try {
                if(!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        conn = null;
    }

    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.4.197:1521:ktfsas1";
        String id = "hadoop_user";
        String pwd = "hadoop_user";
        Connection connection  = OracleConnector.getConnection(url, id, pwd);

        System.out.println(connection.toString());

        OracleConnector.close();
    }


}

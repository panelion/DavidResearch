package com.nexr.platform.search.searcher;

import com.nexr.platform.search.client.*;
import com.nexr.platform.search.client.query.Queries;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 11. 1.
 * Time: 오후 3:13
 */
public class NexrClientSearcher {


    public static void main(String[] args) {

        String clusterName = "nexr_david";

        Properties properties = new Properties();
        properties.put("cluster.name", clusterName);
        properties.put("client", "true");
        properties.put("stage.type", "local");

        String ip = "localhost";
        int port = 9300;


        SearchConnection connection = null;

        try {
            connection = SearchConnectionManager.getConnection(ip, port, properties);
        } catch (SearchException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        if(connection != null) {
            try {
                PagingSearchStatement st = connection.createPagingSearchStatement();

                // st.setPageNumber(1);
                // st.setMaxPageSize(100);
                st.setPageNumber(1);
                st.setIndex("data_2011070500010000");
                st.setType("wcd");

                PagingSearchResultSet resultSet = st.executeQuery(Queries.term("I_CTN", "01042241137"));

                while(resultSet != null && resultSet.next()) {
                    System.out.println("logID : " + resultSet.getRecordID().logID());
                    System.out.println(resultSet.getRecordID().logID());
                }

            } catch (SearchException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }
}

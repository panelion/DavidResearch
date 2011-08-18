package com.nexr.platform.search.searcher;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.*;
import java.util.Properties;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.termQuery;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 18.
 * Time: 오전 8:40
 */
public class CdrQuerySearcher {

    public static QueryBuilder cdrQuery = boolQuery()
            .must(rangeQuery("I_CALL_DT").from("11/07/05").to("11/07/05"))
            .must(rangeQuery("I_ETL_DT").from("11/07/05").to("11/07/05"))
            .must(termQuery("I_CTN", "01077948054"));

    public static void main(String[] args) {

        String phone_list_file_path, serverIP, clusterName;
        int port;

        if(args.length > 0) {
            phone_list_file_path = args[0];
            serverIP = args[1];
            port = Integer.parseInt(args[2]);
            clusterName = args[3];
        } else {
            phone_list_file_path = "/Users/david/Execute/Data/test/phone_number.csv";
            serverIP = "localhost";
            port = 9300;
            clusterName = "nexr";
        }

        File file = new File(phone_list_file_path);
        BufferedReader reader = null;
        if(file.exists()) {
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("can't find phone number file.");
            System.exit(-1);
        }

        Client client = new TransportClient(settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client", "true")
        ).addTransportAddress(new InetSocketTransportAddress(serverIP, port));

        try {
            String row;
            SearchResponse response = null;
            long totalQueryTime = 0L, totalCount = 0L;
            while((row = reader.readLine()) != null) {
                QueryBuilder cdrQuery = boolQuery()
                    .must(rangeQuery("I_CALL_DT").from("11/07/05").to("11/07/05"))
                    .must(rangeQuery("I_ETL_DT").from("11/07/05").to("11/07/06"))
                    .must(termQuery("I_CTN", row));

                response = client.prepareSearch().setQuery(cdrQuery.buildAsBytes()).execute().actionGet();

                totalQueryTime += response.getTookInMillis();
                totalCount++;
            }

            System.out.println(totalCount + " AVERAGE TIME (MILLISECONDS)  : " + String.format("%d", totalQueryTime / totalCount));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

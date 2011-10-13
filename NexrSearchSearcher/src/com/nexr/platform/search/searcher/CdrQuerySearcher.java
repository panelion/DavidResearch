package com.nexr.platform.search.searcher;

import com.nexr.platform.search.client.transport.NexRTransportClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 18.
 * Time: 오전 8:40
 */
public class CdrQuerySearcher {


    private NexRTransportClient _nodeClient;
    private ArrayList<String> _phoneList;
    private volatile int totalCount;
    private Random random;

    private final String[] fields = new String[] {
                 "I_CTN",
                 "I_CALL_DT",
                 "I_OUT_CTN",
                 "I_OUT_CC",
                 "I_IN_CTN",
                 "I_IN_CC",
                 "I_HLR",
                 "I_BONBU",
                 "I_CALLING_SWITCH",
                 "I_CALLED_SWITCH",
                 "I_BSC",
                 "I_IN_ROUTE",
                 "I_OUT_ROUTE",
                 "I_PORTABIL_NO",
                 "I_PORTABIL_ORG",
                 "I_SUBSCRIBER_TYPE",
                 "I_RELEASE_TIME",
                 "I_SERVICE_GRP",
                 "I_SERVICE",
                 "I_CFC_GRP",
                 "I_CFC",
                 "I_CFC_TYPE",
                 "I_INOUT",
                 "I_NET_CLS",
                 "I_PREFIX",
                 "AMT_CALL",
                 "AMT_PDD_CALL",
                 "I_TARGET_ORG",
                 "I_BASIC_SERVICE",
                 "I_DURATION",
                 "I_MNP_2",
                 "I_WCDMA_FCI",
                 "I_CELL",
                 "I_SECTOR",
                 "I_SWITCH"
        };

    public CdrQuerySearcher(String serverList, String clusterName) {

        /*TransportClient transportClient = new TransportClient(settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client", "true")
        );*/

        random = new Random(System.currentTimeMillis());

        Settings finalSettings = settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client", true)
                .put("stage.type", "local").build();

        NexRTransportClient transportClient = NexRTransportClient.create(finalSettings);

        String[] lists = serverList.split(",");

        for(String list : lists) {
            String[] serverInfo = list.trim().split(":");
            if(serverInfo.length == 2) transportClient.addTransportAddress(new InetSocketTransportAddress(serverInfo[0], Integer.parseInt(serverInfo[1])));

        }

        for(DiscoveryNode node : transportClient.connectedNodes()) {
            System.out.println(node.address());
        }

        _nodeClient = transportClient;
    }

    public void close() {
        _nodeClient.close();
    }

    public void loadPhoneNumberFile(String phone_list_file_path) {
        _phoneList = new ArrayList<String>();

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

        String row;
        try {

            totalCount = 0;
            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {
                    _phoneList.add(row);
                    totalCount++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getRandomPhoneNumber() {
        int rndCount = random.nextInt(totalCount);
        return _phoneList.get(rndCount);
    }

    public String search(String phoneNum) {

        QueryBuilder cdrQuery = boolQuery()
            .must(rangeQuery("I_CALL_DT").from("1309791600000").to("1309877999000"))
            .must(rangeQuery("I_ETL_DT").from("1309791600000").to("1309964399000"))
            .must(termQuery("I_CTN", phoneNum)
            );


        SearchResponse response = _nodeClient.search().prepareSearch().setPreference("_primary").setQuery(cdrQuery.buildAsBytes()).addFields(fields).setSize(300).execute().actionGet();

        System.out.println(response.getHits().getHits().length);

        System.out.println(phoneNum + " search response Took Time is : " + response.getTookInMillis() + " ms");
        return phoneNum + " search response Took Time is : " + response.getTookInMillis() + " ms";
        // totalQueryTime += response.getTookInMillis();
    }

    public static void main(String[] args) {

        String phone_list_file_path, serverList, clusterName;

        int runCount = 1000000;

        if(args.length > 0) {
            phone_list_file_path = args[0];
            serverList = args[1];
            clusterName = args[2];
            // runCount = Integer.parseInt(args[4]);

        } else {
            phone_list_file_path = "/Users/david/Execute/Data/test/phone_number.csv";
            // serverList = "192.168.4.92:9200";

            serverList = "192.168.4.92:9300";
            clusterName = "nexr_david";
            // runCount = 10;
        }

        CdrQuerySearcher cdrQuerySearcher = new CdrQuerySearcher(serverList, clusterName);
        cdrQuerySearcher.loadPhoneNumberFile(phone_list_file_path);

       for(int i = 0 ; i < runCount; i++)
        System.out.println(cdrQuerySearcher.search(cdrQuerySearcher.getRandomPhoneNumber()));

        cdrQuerySearcher.close();

        /*SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            System.out.println("I_CALL_DT START TIME : " + format.parse("20110705000000").getTime());
            System.out.println("I_CALL_DT END TIME : " + format.parse("20110705235959").getTime());
            System.out.println("I_ETL_DT START TIME : " + format.parse("20110705000000").getTime());
            System.out.println("I_ETL_DT END TIME : " + format.parse("20110706235959").getTime());

        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/
    }
}

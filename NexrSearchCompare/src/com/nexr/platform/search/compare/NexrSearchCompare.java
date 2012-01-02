package com.nexr.platform.search.compare;

import com.nexr.platform.search.entity.CdrResultEntity;
import com.panelion.utils.db.OracleConnector;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.termQuery;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 22.
 * Time: 오후 3:16
 *
 * Nexr Search 의 검색 결과와, Oracle 의 쿼리 검색 결과를 비교 분석 한다.
 */
public class NexrSearchCompare {


    public Map<CdrResultEntity, Map<String, String>> getResultForOracle(String url ,String id, String pwd, String query) {
        Connection connection = OracleConnector.getConnection(url, id, pwd);
        Statement stmt = null;
        ResultSet resultSet = null;

        Map<CdrResultEntity, Map<String, String>> rtnVal = new HashMap<CdrResultEntity, Map<String, String>>();

        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(query);

            if(resultSet != null) {

                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                while(resultSet.next()) {
                    Map<String, String> map = new HashMap<String, String>();
                    for(int i = 1; i < resultSetMetaData.getColumnCount() + 1; i++) {
                        map.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                    }

                    CdrResultEntity entity = new CdrResultEntity(map.get("I_OUT_CTN"), map.get("I_IN_CTN"), map.get("I_RELEASE_TIME"));
                    rtnVal.put(entity, map);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                resultSet.close();
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            OracleConnector.close();
        }

        return rtnVal;
    }

    public Map<CdrResultEntity, Map<String, String>> getResultForNexr(String clusterName, String serverIP, int port, String startDate, String endDate, String phoneNumber) {

        Map<CdrResultEntity, Map<String, String>> rtnVal = new HashMap<CdrResultEntity, Map<String, String>>();

        Client client = new TransportClient(settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client", "true")
        ).addTransportAddress(new InetSocketTransportAddress(serverIP, port));

        String[] fields = new String[] {
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
                 "U_CELL",
                 "SECTOR",
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
                 "I_WCDMA_FCI"
        };

        QueryBuilder cdrQuery = boolQuery()
                        .must(rangeQuery("I_CALL_DT").from("11/07/05").to("11/07/05"))
                        .must(rangeQuery("I_ETL_DT").from("11/07/05").to("11/07/06"))
                        .must(termQuery("I_CTN", phoneNumber))
                        // .must(termQuery("I_SERVICE_GRP" , "10"))
                        ;

        int totalCount = 0;
        int size = 10;
        Long getTotalHits = 0L;

        do {
            SearchResponse response = client.prepareSearch()
                    .setQuery(cdrQuery.buildAsBytes())
                    .addSort("I_CALL_DT", SortOrder.DESC)
                    .addScriptField("AMT_PDD_CALL", "_fields['AMT_PDD_CALL'].value / 10")
                    .addScriptField("AMT_CALL", "_fields['AMT_CALL'].value / 10")
                    .addScriptField("I_DURATION", "_fields['I_DURATION'].value / 10.0")
                    .addScriptField("I_INOUT", "if(_fields['I_INOUT'].value == 0) { return '발신' } else { return '착신' };")
                    .addFields(fields)
                    .setFrom(totalCount)
                    .setSize(size)

                .execute().actionGet();

            if(response.getShardFailures().length > 0) {
                getTotalHits = 0L;
            } else {
                getTotalHits = response.getHits().getTotalHits();
            }

            int hitSize = response.getHits().hits().length;

            for(int i = 0 ; i < hitSize; i++) {
                SearchHit searchHits = response.hits().getAt(i);

                Map<String, String> map = new HashMap<String, String>();
                for(Map.Entry<String, SearchHitField> entry : searchHits.getFields().entrySet()){
                    map.put(entry.getValue().getName(), entry.getValue().values().get(entry.getValue().values().size() - 1).toString());
                }

                CdrResultEntity entity = new CdrResultEntity(map.get("I_OUT_CTN"), map.get("I_IN_CTN"), map.get("I_RELEASE_TIME"));
                rtnVal.put(entity, map);
            }

            totalCount += hitSize;

        } while(getTotalHits > totalCount);

        return rtnVal;
    }

    public String getOracleQuery(String phoneNumber) {
        return
                " SELECT" +
                " to_char(SV.I_CALL_DT, 'yy/mm/dd') I_CALL_DT,\n" +
                " SV.I_OUT_CTN,\n" +
                " SV.I_OUT_CC,\n" +
                " SV.I_IN_CTN,\n" +
                " SV.I_IN_CC,\n" +
                " SV.I_HLR,\n" +
                " SV.I_BONBU,\n" +
                " SV.I_CALLING_SWITCH,\n" +
                " SV.I_CALLED_SWITCH,\n" +
                " SV.I_BSC,\n" +
                " decode(SC.u_cell, null, sv.i_cell, SC.u_cell) U_CELL,\n" +
                " nvl((select t_sec from sd_com_sec where i_sec = SV.I_SECTOR), '-') SECTOR,\n" +
                " SV.I_IN_ROUTE,\n" +
                " SV.I_OUT_ROUTE,\n" +
                " SV.I_PORTABIL_NO,\n" +
                " SV.I_PORTABIL_ORG,\n" +
                " SV.I_SUBSCRIBER_TYPE,\n" +
                " SV.I_RELEASE_TIME,\n" +
                " SV.I_SERVICE_GRP,\n" +
                " SV.I_SERVICE,\n" +
                " SV.I_CFC_GRP,\n" +
                " SV.I_CFC,\n" +
                " SV.I_CFC_TYPE,\n" +
                " decode(SV.I_INOUT, '0', '발신', '1', '착신') I_INOUT, \n" +
                " SV.I_NET_CLS,                 \n" +
                " SV.I_PREFIX,\n" +
                " SV.AMT_CALL,\n" +
                " SV.AMT_PDD_CALL,\n" +
                " SV.I_TARGET_ORG,\n" +
                " SV.I_BASIC_SERVICE,\n" +
                " SV.I_DURATION,\n" +
                " SV.I_MNP_2,\n" +
                " SV.I_WCDMA_FCI\n" +
                " FROM ( \n" +
                "   SELECT /*+ index(srf_wcd_voice srf_wcd_voice_idx02 ) */   I_CALL_DT              \n" +
                "  ,I_OUT_CTN                \n" +
                "  ,I_OUT_CC                 \n" +
                "  ,I_IN_CTN                 \n" +
                "  ,I_IN_CC                  \n" +
                "  ,I_HLR                \n" +
                "  ,I_BONBU                  \n" +
                "  ,I_CALLING_SWITCH         \n" +
                "  ,I_CALLED_SWITCH          \n" +
                "  ,I_BSC                \n" +
                "  ,i_cell                   \n" +
                "  ,I_SECTOR             \n" +
                "  ,I_IN_ROUTE               \n" +
                "  ,I_OUT_ROUTE              \n" +
                "  ,I_PORTABIL_NO            \n" +
                "  ,I_PORTABIL_ORG           \n" +
                "  ,I_SUBSCRIBER_TYPE                \n" +
                "  ,I_RELEASE_TIME                   \n" +
                "  ,I_SERVICE_GRP                    \n" +
                "  ,I_SERVICE                        \n" +
                "  ,I_CFC_GRP                        \n" +
                "  ,I_CFC                            \n" +
                "  ,I_CFC_TYPE                       \n" +
                "  ,I_INOUT                          \n" +
                "  ,I_NET_CLS                        \n" +
                "  ,I_PREFIX                     \n" +
                "  ,AMT_CALL/10 AMT_CALL         \n" +
                "  ,AMT_PDD_CALL/10 AMT_PDD_CALL \n" +
                "  ,I_TARGET_ORG                 \n" +
                "  ,I_BASIC_SERVICE                  \n" +
                "  ,I_DURATION/10  I_DURATION        \n" +
                "  ,I_MNP_2                          \n" +
                "  ,I_WCDMA_FCI                      \n" +
                "    FROM srf_wcd_voice \n" +
                "   WHERE             \n" +
                "(i_call_dt||'' between to_date('11/07/05', 'yy/mm/dd')  " +
                    "and to_date('11/07/05',  'yy/mm/dd') " +
                    "AND i_etl_dt between to_date('11/07/05', 'yy/mm/dd')  " +
                    "and to_date('11/07/05',  'yy/mm/dd')+1 )  " +
                    "and (i_inout in ('0') " +
                    "and i_out_ctn in ('" + phoneNumber + "'))  \n" +
                "UNION  ALL \n" +
                "   SELECT /*+ index(srf_wcd_voice srf_wcd_voice_idx01 ) */   I_CALL_DT              \n" +
                "  ,I_OUT_CTN                \n" +
                "  ,I_OUT_CC                 \n" +
                "  ,I_IN_CTN                 \n" +
                "  ,I_IN_CC                  \n" +
                "  ,I_HLR                \n" +
                "  ,I_BONBU                  \n" +
                "  ,I_CALLING_SWITCH         \n" +
                "  ,I_CALLED_SWITCH          \n" +
                "  ,I_BSC                \n" +
                "  ,i_cell                   \n" +
                "  ,I_SECTOR             \n" +
                "  ,I_IN_ROUTE               \n" +
                "  ,I_OUT_ROUTE              \n" +
                "  ,I_PORTABIL_NO            \n" +
                "  ,I_PORTABIL_ORG           \n" +
                "  ,I_SUBSCRIBER_TYPE                \n" +
                "  ,I_RELEASE_TIME                   \n" +
                "  ,I_SERVICE_GRP                    \n" +
                "  ,I_SERVICE                        \n" +
                "  ,I_CFC_GRP                        \n" +
                "  ,I_CFC                            \n" +
                "  ,I_CFC_TYPE                       \n" +
                "  ,I_INOUT                          \n" +
                "  ,I_NET_CLS                        \n" +
                "  ,I_PREFIX                     \n" +
                "  ,AMT_CALL/10 AMT_CALL         \n" +
                "  ,AMT_PDD_CALL/10 AMT_PDD_CALL \n" +
                "  ,I_TARGET_ORG                 \n" +
                "  ,I_BASIC_SERVICE                  \n" +
                "  ,I_DURATION/10  I_DURATION        \n" +
                "  ,I_MNP_2                          \n" +
                "  ,I_WCDMA_FCI                      \n" +
                "    FROM srf_wcd_voice \n" +
                "   WHERE             \n" +
                "(i_call_dt||'' between to_date('11/07/05', 'yy/mm/dd')  " +
                        "and to_date('11/07/05',  'yy/mm/dd') " +
                        "AND i_etl_dt between to_date('11/07/05', 'yy/mm/dd')  " +
                        "and to_date('11/07/05',  'yy/mm/dd') + 1 )  " +
                        "and (i_inout in  ('1') " +
                        "and i_in_ctn in ('" + phoneNumber + "'))) sv, \n" +
                "sd_com_cell SC \n" +
                " WHERE  DECODE(SV.I_INOUT, '0', SV.I_CALLING_SWITCH,  '2', SV.I_CALLING_SWITCH,'4'," +
                        " SV.I_CALLING_SWITCH, SV.I_CALLED_SWITCH) = SC.I_SWITCH (+)              \n" +
                // " AND    SV.i_service_grp = '10'          \n" +
                " AND    SV.I_BSC = SC.I_BSC(+)          \n" +
                " AND    SV.I_CELL = SC.I_CELL(+)        \n" +
                " AND    SC.I_ENDT(+) = '99991231'       \n" +
                " ORDER BY 1 DESC";
    }

    public void compare(Map<CdrResultEntity, Map<String, String>> oracleData, Map<CdrResultEntity, Map<String, String>> nexrData) {

        int totalCount = 1 ;
        for(Map.Entry<CdrResultEntity, Map<String, String>> map : oracleData.entrySet()) {

            System.out.println("*************************** " + totalCount + " RESULT *****************************");

            if(nexrData.containsKey(map.getKey())) {

                Map<String, String> oracleMap = map.getValue();
                Map<String, String> nexrMap = nexrData.get(map.getKey());

                for(Map.Entry<String, String> oracle : oracleMap.entrySet()) {

                    String oracleKey = oracle.getKey();
                    String oracleVal = oracle.getValue() == null ? "" : oracle.getValue();

                    String nexrVal = "";
                    if(nexrMap.containsKey(oracleKey)) {
                        nexrVal = nexrMap.get(oracleKey);
                    }

                    if(!oracleVal.equalsIgnoreCase(nexrVal)) {
                        System.out.println("[" + oracleKey + "] " + "ORACLE : " + oracleVal + " NEXR : " + nexrVal);
                    }
                }

                System.out.println("[  발신 번호 ] " + " [   착신 번호  ] " + " [  발착신 구분 번호  ]");
                System.out.println("[" + nexrMap.get("I_OUT_CTN") + "] " + " [ " + nexrMap.get("I_IN_CTN") + " ] " + " [ " + nexrMap.get("I_CTN") + " ]");

            } else {
                System.out.println("CAN'T FIND EQUALS NEXR SEARCH DATA.");
                System.out.println("ORACLE DATA IS, ");
                System.out.println(map.getKey().toString());
            }
            totalCount++;
        }
    }

    public static void main(String[] args) {
        String oracle_url = "jdbc:oracle:thin:@192.168.4.197:1521:ktfsas1";
        String oracle_id = "hadoop_user";
        String oracle_pwd = "hadoop_user";
        // String query = "select * from srf_wcd_voice where rownum < 10";

        String clusterName = "nexr";
        String serverIP = "192.168.4.197";
        int port = 12083;
        String startDate = "11/07/05";
        String endDate = "11/07/06";


        String phoneNumber = "01035833969";

        NexrSearchCompare compare = new NexrSearchCompare();

        Map<CdrResultEntity, Map<String, String>> oracleResult = compare.getResultForOracle(oracle_url, oracle_id, oracle_pwd, compare.getOracleQuery(phoneNumber));
        Map<CdrResultEntity, Map<String, String>> nexrResult = compare.getResultForNexr(clusterName, serverIP, port, startDate, endDate, phoneNumber);


        System.out.println("*************************************************");
        System.out.println("Compare Oracle VS Nexr Search.");
        System.out.println("*************************************************");

        System.out.println("oracle Query Count : " + oracleResult.size() + " , NexR Search Query Count : " + nexrResult.size());

        compare.compare(oracleResult, nexrResult);
    }

}

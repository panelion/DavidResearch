package com.nexr.platform.search.Quries;

import com.nexr.platform.search.client.action.search.NexRSearchRequestBuilder;
import com.panelion.utils.json.JSONException;
import com.panelion.utils.json.JSONObject;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 1/2/12
 * Time: 3:26 PM
 */
public class q13 extends AbstractIQueries {

    public q13() {
        _logger = Logger.getLogger(q13.class);
        START_DATE = "20110705";
        END_DATE = "20110706";
    }
    @Override
    /**
     * @{PHONE_NUMBER}  should to replacement this value.
     */
    public String getOracleQuery() {
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
                        "and i_out_ctn in ('@{PHONE_NUMBER}'))  \n" +
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
                        "and i_in_ctn in ('@{PHONE_NUMBER}'))) sv, \n" +
                        "sd_com_cell SC \n" +
                        " WHERE  DECODE(SV.I_INOUT, '0', SV.I_CALLING_SWITCH,  '2', SV.I_CALLING_SWITCH,'4'," +
                        " SV.I_CALLING_SWITCH, SV.I_CALLED_SWITCH) = SC.I_SWITCH (+)              \n" +
                        // " AND    SV.i_service_grp = '10'          \n" +
                        " AND    SV.I_BSC = SC.I_BSC(+)          \n" +
                        " AND    SV.I_CELL = SC.I_CELL(+)        \n" +
                        " AND    SC.I_ENDT(+) = '99991231'       \n" +
                        " ORDER BY 1 DESC";
    }

    @Override
    public String getOracleQuery(String phoneNumber) {
        return null;
    }

    @Override
    public String getOracleQuery(String I_CALL_DT, String I_ETL_DT, String... phoneNumber) {
        return null;
    }

    @Override
    public NexRSearchRequestBuilder getNexrSearchQuery() {
        return getNexrSearchQuery("");
    }

    @Override
    public NexRSearchRequestBuilder getNexrSearchQuery(String... phoneNumber) {
        return getNexrSearchQuery(this.START_DATE, this.END_DATE, phoneNumber);
    }

    @Override
    public NexRSearchRequestBuilder getNexrSearchQuery(String I_CALL_DT, String I_ETL_DT, String... phoneNumber) {
        return null;
    }

    @Override
    public JSONObject getSearchResponseByRest() {
        return this.getSearchResponseByRest("");
    }

    @Override
    public JSONObject getSearchResponseByRest(String... phoneNumber) {
        return this.getSearchResponseByRest(this.START_DATE, this.END_DATE, phoneNumber);
    }

    @Override
    public JSONObject getSearchResponseByRest(String I_CALL_DT, String I_ETL_DT, String... phoneNumber) {

        String url = BASE_URL + "/cdr_q13?start_date=" + I_CALL_DT + "&end_date=" + I_ETL_DT;

        for(String str : phoneNumber) {
            url = url + "&phone_number=" + str;
        }

        JSONObject object = null;
        try {
            object = this.parseToJson(this.getResponseBody(url));
        } catch (IOException e) {
            _logger.error(e.getMessage());
        } catch (JSONException e) {
            _logger.error(e.getMessage());
        }

        return object;
    }
}

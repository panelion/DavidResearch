package com.nexr.platform.search.Quries;

import com.nexr.platform.search.client.action.search.NexRSearchRequestBuilder;
import com.panelion.utils.json.JSONObject;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 1/2/12
 * Time: 3:22 PM
 */
public interface IQueries {

    public String getOracleQuery();
    public String getOracleQuery(String phoneNumber);
    public String getOracleQuery(String I_CALL_DT, String I_ETL_DT, String... phoneNumber);

    public NexRSearchRequestBuilder getNexrSearchQuery();
    public NexRSearchRequestBuilder getNexrSearchQuery(String... phoneNumber);
    public NexRSearchRequestBuilder getNexrSearchQuery(String I_CALL_DT, String I_ETL_DT, String... phoneNumber);
    
    public JSONObject getSearchResponseByRest();
    public JSONObject getSearchResponseByRest(String... phoneNumber);
    public JSONObject getSearchResponseByRest(String I_CALL_DT, String I_ETL_DT, String... phoneNumber);

}

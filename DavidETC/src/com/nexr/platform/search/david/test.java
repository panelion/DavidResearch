package com.nexr.platform.search.david;

import com.panelion.utils.json.JSONArray;
import com.panelion.utils.json.JSONException;
import com.panelion.utils.json.JSONObject;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 11. 2.
 * "time" : 오전 9:36
 * To change this template use File | Settings | File Templates.
 */
public class test {
    public static void main(String[] args) throws JSONException {

        ArrayList<String> timestampList = new ArrayList<String>();

        String strJson = "{\"entries\": [\n" +
                "{\n" +
                "\"time\": 1320019200000,\n" +
                "\"count\": 2\n" +
                "},\n" +
                "{\n" +
                "\"time\": 1320105600000,\n" +
                "\"count\": 361760\n" +
                "}\n" +
                "]\n" +
                "}\n" +
                "}";
        JSONObject jsonObject = new JSONObject(strJson);

        JSONArray array = (JSONArray)jsonObject.get("entries");


        SimpleDateFormat sdfCurrent = new SimpleDateFormat ("yyyy-MM-dd");
        
        for(int i = 0 ; i < array.length(); i++){
            JSONObject obj = (JSONObject)array.get(i);
            Timestamp currentTime = new Timestamp(Long.parseLong(String.valueOf(obj.get("time"))));
            String today = sdfCurrent.format(currentTime);
            
            System.out.println(today + " :: " + obj.get("count"));
        }
    }
}
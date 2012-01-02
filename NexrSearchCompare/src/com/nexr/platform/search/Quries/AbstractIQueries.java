package com.nexr.platform.search.Quries;

import com.panelion.utils.json.JSONException;
import com.panelion.utils.json.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 1/2/12
 * Time: 3:47 PM
  */
public abstract class AbstractIQueries implements IQueries {
    
    private static DateTimeFormatter format8 = DateTimeFormat.forPattern("yyyyMMdd");
    private static DateTimeFormatter format10 = DateTimeFormat.forPattern("yyyyMMddHH");
    private static DateTimeFormatter format12 = DateTimeFormat.forPattern("yyyyMMddHHmm");
    private static DateTimeFormatter format14 = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    
    protected String BASE_URL;
    protected String START_DATE;
    protected String END_DATE;

    protected Logger _logger = null;

    public final static long toDate(String dateString) {

        dateString = dateString.replaceAll("\\D", "");
        
        switch (dateString.length()) {
            case 8: // yyyyMMdd
                return format8.parseMillis(dateString);
            case 10: // yyyyMMddHH
                return format10.parseMillis(dateString);
            case 12: // yyyyMMddHHmm
                return format12.parseMillis(dateString);
            case 14: // yyyyMMddHHmmss
                return format14.parseMillis(dateString);
        }
        return 0;
    }
    
    public final void setBaseURL(String url) {
        BASE_URL = url;        
    }
    
    public final String getBaseURL() {
        return this.BASE_URL;
    }

    protected String getResponseBody(String url) throws IOException {

        HttpClient httpClient = new HttpClient();
        GetMethod method = new GetMethod(url);

        try {
            int statusCode = httpClient.executeMethod(method);

            if(statusCode != HttpStatus.SC_OK) {
                _logger.warn(method.getResponseBody());
                return "";
            }

            return  new String(method.getResponseBody());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }
    
    protected JSONObject parseToJson(String body) throws JSONException {
        return new JSONObject(body);
    }
    
    public static void main(String... args){
        System.out.println(AbstractIQueries.toDate("2011-12-12 00:00:00"));
    }
}

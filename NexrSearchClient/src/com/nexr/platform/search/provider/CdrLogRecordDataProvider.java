package com.nexr.platform.search.provider;

import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.entity.SCKeyEntity;
import com.nexr.platform.search.router.MapRoutingEvent;
import com.nexr.platform.search.router.RoutingEvent;
import com.panelion.utils.DateUtils;
import com.panelion.utils.ValidateUtils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.addAll;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 12.
 * Time: 오후 9:25
 * Log File 에서 Data 를 읽어, LogRecordKey, LogRecord 의 Map 형태로 생성 한다.
 */
public class CdrLogRecordDataProvider extends StreamDataProvider<RoutingEvent> {

    private final String _ENCODING = "EUC-KR";
    private final String ROUTING_EVENT_DATA_TYPE = "routing.event.data.type";

    private Properties _prof;
    private ArrayList<String> _arrColumnData, _arrUsedColumnData;

    private Map<SCKeyEntity, Map<String, String>> _mapComCell;
    private ArrayList<Map<String, String>> _mapComSec;
    private BufferedReader _dataReader;

    private final String _prefixLogId;

    public CdrLogRecordDataProvider(Properties prof, String columnFilePath, String dataFilePath
            , String serverIP, String sdComCellFilePath, String sdComSecFilePath, String usedColumnFilePath)
            throws Exception {

        _prof = prof;
        _arrColumnData = new ArrayList<String>();

        try {
            _arrColumnData = this.loadColumnData(columnFilePath);
            _arrUsedColumnData = this.loadColumnData(usedColumnFilePath);

            File file = new File(dataFilePath);
            if(file.exists()) {
                _dataReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            }

            this.load_sd_com_cell(sdComCellFilePath);
            this.load_sd_com_sec(sdComSecFilePath);

        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }

        DateUtils dateUtils = new DateUtils("yyyyMMddHHmmss");
        _prefixLogId = serverIP.replaceAll("\\.", "-") + "-" + dateUtils.getCurrentTime() + "-";
        _produceCount = 0;
    }

    public void load_sd_com_cell(String filePath) throws IOException {
        _mapComCell = this.loadSCFileData(filePath);
    }

    public void load_sd_com_sec(String filePath) throws IOException {
        _mapComSec = this.loadFileData(filePath);
    }

    /**
     * Column 명이 정의 되어 있는 파일을 읽어, ArrayList<String> 타입 으로 저장 한다.
     * @param columnFilePath Column Define File Path
     * @throws IOException  컬럼 파일 이 없거나, 읽을 수 없을 경우에 에러가 난다.
     * @return  ArrayList<String>
     */
    private ArrayList<String> loadColumnData(String columnFilePath) throws IOException {
        File file = new File(columnFilePath);
        ArrayList<String> rtnVal = new ArrayList<String>();
        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

            String row;
            while((row = reader.readLine()) != null){
                if(!row.isEmpty()) rtnVal.add(row.trim());
            }
        } else {
            throw new IOException("Can't find File : " + columnFilePath);
        }

        return rtnVal;
    }

    private ArrayList<Map<String, String>> loadFileData(String filePath) throws IOException {
        File file = new File(filePath);
        String SEPARATOR = "\t";

        ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();

        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
            String row;
            int rowCount = 0;

            ArrayList<String> columnDefine = new ArrayList<String>();
            Map<String, String> map;
            while((row = reader.readLine()) != null) {
                row = row.trim();
                if(!row.isEmpty()) {
                    map = new HashMap<String, String>();
                    String[] rows = row.split(SEPARATOR);

                    if(rowCount == 0) {
                        addAll(columnDefine, rows);
                    } else {
                        for(int i = 0; i < columnDefine.size(); i++) {
                            map.put(columnDefine.get(i), ValidateUtils.getValidValue(rows[i]));
                        }

                        list.add(map);
                    }
                    rowCount++;
                }
            }
        }
        return list;
    }

    /**
     * 파일의 정보를 읽어 들인다.
     * 첫줄은 Column 정보 이며,
     * 나머지 줄은 Data 가 된다.
     * @param filePath  파일의 위치
     * @return  Map
     * @throws java.io.IOException  IOException
     */
    private Map<SCKeyEntity, Map<String, String>> loadSCFileData(String filePath) throws IOException {
        File file = new File(filePath);

        String SEPARATOR = "\t";

        Map<SCKeyEntity, Map<String, String>> joinMap = new HashMap<SCKeyEntity, Map<String, String>>();

        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
            String row;
            int rowCount = 0;

            ArrayList<String> columnDefine = new ArrayList<String>();

            Map<String, String> map;
            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {

                    map = new HashMap<String, String>();
                    String[] rows;

                    if(rowCount == 0) {
                        rows = row.split(SEPARATOR);
                        addAll(columnDefine, rows);
                    } else {
                        rows = row.split(SEPARATOR, columnDefine.size());
                        for(int i = 0; i < columnDefine.size(); i++) {
                            try {
                                map.put(columnDefine.get(i), rows[i] == null ? "" : rows[i]);
                            } catch(Exception e) {
                                map.put(columnDefine.get(i), "");
                            }
                        }
                    }

                    if(rowCount != 0) {
                        SCKeyEntity scKey = new SCKeyEntity(map.get("I_SWITCH"), map.get("I_BSC"), map.get("I_CELL"), map.get("I_ENDT"));
                        joinMap.put(scKey, map);
                    }
                    rowCount++;
                }
            }
        }
        return joinMap;
    }

    @Override
    public DataConsumer.DataEvent<RoutingEvent> next() {

        MapRoutingEvent event = new MapRoutingEvent(_prof.getProperty(ROUTING_EVENT_DATA_TYPE, "TransactionLog"));
        Map<String, String> mapData = new HashMap<String, String>();
        String row = "";

        try {
            row = _dataReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        if(row == null) {
            return null;
        }

        row = row.trim();

        if(row.isEmpty()) {
            return null;
        }

        String[] cols = row.split(",", _arrColumnData.size());

        String key, col;
        for(int i = 0 ; i < _arrColumnData.size(); i++){
            key = _arrColumnData.get(i);
            try {
                col = cols[i].trim();
            } catch(Exception e){
                col = "";
            }
            if(col == null) col = "";
            mapData.put(key, col);
        }

        /**
         * nvl((select t_sec from sd_com_sec where i_sec = SV.I_SECTOR), '-') "SECTOR"
         */
        String sectorVal = mapData.get("I_SECTOR");
        String sector = "-";
        for (Map<String, String> map : _mapComSec) {
            if (map.get("I_SEC").equals(sectorVal)) {
                sector = map.get("T_SEC");
            }
        }

        mapData.put("SECTOR", sector);

        /**
         * I_CTN 생성
         */
        String i_inout = mapData.get("I_INOUT");
        String i_ctn = "";
        if(i_inout.equals("0")) {
            i_ctn = mapData.get("I_IN_CTN");
        } else if(i_inout.equals("1")) {
            i_ctn = mapData.get("I_OUT_CTN");
        }

        mapData.put("I_CTN", i_ctn);

        /**
         * 기지국 생성
         */
        String i_bsc = ValidateUtils.getValidValue(mapData.get("I_BSC"));
        String i_cell = ValidateUtils.getValidValue(mapData.get("I_CELL"));
        String temp_i_switch = "";
        String en_dt = "99991231";

        if(i_inout.equals("0") || i_inout.equals("2") || i_inout.equals("4")) {
            temp_i_switch = ValidateUtils.getValidValue(mapData.get("I_CALLING_SWITCH"));
        } else {
            temp_i_switch = ValidateUtils.getValidValue(mapData.get("I_CALLED_SWITCH"));
        }

        SCKeyEntity scKey = new SCKeyEntity(temp_i_switch, i_bsc, i_cell, en_dt);

        String u_cell = "";

        if(_mapComCell.containsKey(scKey)) {
            u_cell = _mapComCell.get(scKey).get("U_CELL");
        } else {
            u_cell = mapData.get("I_CELL");
        }

        event.put("U_CELL", u_cell);

        event.setId(_prefixLogId + String.format("%09d", _produceCount));
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            event.setTimeStamp(format.parse(mapData.get("I_RELEASE_TIME") + "00").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for(Map.Entry<String, String> entry : mapData.entrySet()){
            if(_arrUsedColumnData.contains(entry.getKey())) event.put(entry.getKey(), entry.getValue());
        }

        // if(_dataCount % 10000 == 0) System.out.println(_dataCount);

        _produceCount++;

        return new DataConsumer.DataEvent<RoutingEvent>(event);
    }

    @Override
    public void reset() {
    }
}

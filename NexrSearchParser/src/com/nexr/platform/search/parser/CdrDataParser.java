package com.nexr.platform.search.parser;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;
import com.nexr.platform.search.utils.io.MapFileWriter;
import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import sun.java2d.pipe.SpanShapeRenderer;

import javax.sql.RowSet;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

/**
 * CSV 타입 으로 된 CDR 데이터 를 파싱 하여, Hadoop FileSystem 에 LogRecordKey, LogRecord 의 Map 형태로 저장 한다.
 */
public class CdrDataParser {

    private final String _ENCODING = "EUC-KR";

    private final String _columnFilePath;
    private final String _dataFilePath;

    private ArrayList<String> _arrColumnData;
    private MapFileWriter _mapFileWriter;

    private Map<SCKey, Map<String, String>> _mapComCell;
    private ArrayList<Map<String, String>> _mapComSec;

    public CdrDataParser(String mapFilePath, String columnFilePath, String dataFilePath){
        _columnFilePath = columnFilePath;
        _dataFilePath = dataFilePath;

        _arrColumnData = new ArrayList<String>();
        _mapFileWriter = new MapFileWriter(mapFilePath);

        try {
            _mapFileWriter.open();
            this.loadColumnData();
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public void load_sd_com_cell(String filePath) throws IOException {
        _mapComCell = this.loadSCFileData(filePath);
    }

    public void load_sd_com_sec(String filePath) throws IOException {
        _mapComSec = this.loadFileData(filePath);
    }

    /**
     * Column 명이 정의 되어 있는 파일을 읽어, ArrayList<String> 타입 으로 저장 한다.
     * @throws IOException  컬럼 파일 이 없거나, 읽을 수 없을 경우에 에러가 난다.
     */
    private void loadColumnData() throws IOException {
        File file = new File(_columnFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

        String row;
        while((row = reader.readLine()) != null){
            if(!row.isEmpty()) _arrColumnData.add(row.trim());
        }
    }

    /**
     * 파일의 정보를 읽어 들인다.
     * 첫줄은 Column 정보 이며,
     * 나머지 줄은 Data 가 된다.
     * @param filePath  파일의 위치
     * @return  Map
     * @throws java.io.IOException  IOException
     */
    private Map<SCKey, Map<String, String>> loadSCFileData(String filePath) throws IOException {
        File file = new File(filePath);

        String SEPARATOR = "\t";

        Map<SCKey, Map<String, String>> joinMap = new HashMap<SCKey, Map<String, String>>();

        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
            String row;
            int rowCount = 0;

            ArrayList<String> columnDefine = new ArrayList<String>();
            Map<String, String> map = new HashMap<String, String>();

            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {

                    String[] rows;
                    if(rowCount == 0) {
                        rows = row.split(SEPARATOR);
                        for(String str : rows) {
                            columnDefine.add(str);
                        }
                    } else {
                        rows = row.split(SEPARATOR, columnDefine.size());
                        for(int i = 0; i < columnDefine.size(); i++) {
                            try {
                                map.put(columnDefine.get(i), rows[i] == null ? "" : rows[i]);
                            } catch(Exception e) {
                                System.err.println(i + " " + columnDefine.get(i));
                                map.put(columnDefine.get(i), "");
                            }
                        }
                    }
                }

                if(rowCount != 0) {
                    SCKey scKey = new SCKey(map.get("I_SWITCH"), map.get("I_BSC"), map.get("I_CELL"), map.get("I_ENDT"));
                    joinMap.put(scKey, map);
                }

                rowCount++;
            }
        }
        return joinMap;
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
            Map<String, String> map = new HashMap<String, String>();

            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {
                    String[] rows = row.trim().split(SEPARATOR);
                    if(rowCount == 0) {
                        for(String str : rows) columnDefine.add(str);
                    } else {
                        for(int i = 0; i < columnDefine.size(); i++) {
                            map.put(columnDefine.get(i), rows[i] == null ? "" : rows[i]);
                        }
                    }

                    list.add(map);
                }
                rowCount++;
            }
        }
        return list;
    }

    private static class SCKey {
        final String iSwitch;
        final String iBsc;
        final String iCell;
        final String iEndt;
        final String all;
        final String separator = "@";

        private SCKey(String iSwitch, String iBsc, String iCell, String iEndt) {
            this.iSwitch = getValidValue(iSwitch) ;
            this.iBsc = getValidValue(iBsc);
            this.iCell = getValidValue(iCell);
            this.iEndt = getValidValue(iEndt);
            this.all = iSwitch + separator + iBsc + separator + iCell + separator + iEndt;
        }

        @Override
        public int hashCode() {
            return all.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof SCKey && all.equals(((SCKey) o).all);
        }
    }

    /**
     * CDR Data 를 파싱 하여, File System 에 작성 한다.
     * @throws IOException  DataFile 을 읽을 수 없을 때, 에러가 발생 한다.
     */
    public void start() throws IOException {
        File file = new File(_dataFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

        String row;
        int dataCount = 0;
        while((row = reader.readLine()) != null){
            LogRecord logRecord = new LogRecord();

            String _SEPARATOR = ",";
            String[] cols = row.split(_SEPARATOR);

            String key;
            String col;
            for(int i = 0 ; i < _arrColumnData.size(); i++){
                key = _arrColumnData.get(i);
                try {
                    col = cols[i].trim();
                } catch(Exception e){
                    col = "";
                }
                if(col == null) col = "";
                logRecord.add(key, col);
            }

            /**
             * nvl((select t_sec from sd_com_sec where i_sec = SV.I_SECTOR), '-') "SECTOR"
             */
            String sectorVal = logRecord.getValue("I_SECTOR");
            String sector = "-";
            for(int j=0; j < _mapComSec.size(); j++) {
                Map<String, String> map = _mapComSec.get(j);
                if(map.get("I_SEC").equals(sectorVal)) {
                    sector = map.get("T_SEC");
                }
            }

            logRecord.add("SECTOR", sector);

            /**
             * I_CTN 생성
             */
            String i_inout = logRecord.getValue("I_INOUT");
            String i_ctn = "";
            if(i_inout.equals("0")) {
                i_ctn = logRecord.getValue("I_IN_CTN");
            } else if(i_inout.equals("1")) {
                i_ctn = logRecord.getValue("I_OUT_CTN");
            }

            logRecord.add("I_CTN", i_ctn);

            /**
             * 기지국 생성
             */
            String i_bsc = getValidValue(logRecord.getValue("I_BSC"));
            String i_cell = getValidValue(logRecord.getValue("I_CELL"));
            String temp_i_switch = "";
            String en_dt = "99991231";

            if(i_inout.equals("0") || i_inout.equals("2") || i_inout.equals("4")) {
                temp_i_switch = getValidValue(logRecord.getValue("I_CALLING_SWITCH"));
            } else {
                temp_i_switch = getValidValue(logRecord.getValue("I_CALLED_SWITCH"));
            }

            SCKey sckey = new SCKey(temp_i_switch, i_bsc, i_cell, en_dt);

            String u_cell = "";

            if(_mapComCell.containsKey(sckey)) {
                u_cell = _mapComCell.get(sckey).get("U_CELL");
            } else {
                u_cell = logRecord.getValue("I_CELL");
            }

            logRecord.add("U_CELL", u_cell);

            LogRecordKey logRecordKey = new LogRecordKey();

            logRecordKey.setLogId(String.format("%09d", dataCount++));
            //call dt -> yy/MM/dd -> yyyymmdd
            try {
                logRecordKey.setTime(LogRecordKey.formatter.format(format.parse(logRecord.getValue("I_RELEASE_TIME") + "00")));
            } catch (ParseException e) {
                continue;
            }
            logRecordKey.setDataType(logRecord.getValue("I_SERVICE"));


            _mapFileWriter.getMapFileWriter().append(logRecordKey, logRecord);

            if(dataCount % 10000 == 0) System.out.println(dataCount);
        }
    }

    private static String getValidValue(String value) {
        return isNull(value) ? "null" : value;
    }

    private static boolean isNull(String value) {
        return value == null || value.trim().length() == 0 || value.equalsIgnoreCase("null");
    }

    public static void main(String[] args){

        String mapFilePath, columnFilePath, dataFilePath, sdComCellFilePath, sdComSecFilePath;

        if(args.length > 0){

            mapFilePath = args[0];
            columnFilePath = args[1];
            dataFilePath = args[2];
            sdComCellFilePath = args[3];
            sdComSecFilePath = args[4];

        } else {

            mapFilePath = "/Users/david/Data/SearchPlatform/CDR/hdfs/";
            columnFilePath = "/Users/david/Execute/elasticsearch_client/data/cdr/cdr_column.csv";
            dataFilePath = "/Users/david/Data/SearchPlatform/CDR/srf_wcd_voice_1_2.csv";

            sdComCellFilePath = "/Users/david/Execute/elasticsearch_client/data/cdr/sd_com_cell.txt";
            sdComSecFilePath = "/Users/david/Execute/elasticsearch_client/data/cdr/sd_com_sec.txt";

        }

        try{
            CdrDataParser parser = new CdrDataParser(mapFilePath, columnFilePath, dataFilePath);
            parser.load_sd_com_cell(sdComCellFilePath);
            parser.load_sd_com_sec(sdComSecFilePath);
            parser.start();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}

package com.nexr.search.platform.parser;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;
import com.nexr.search.platform.parser.io.MapFileWriter;

import java.io.*;
import java.util.ArrayList;

/**
 * CSV 타입 으로 된 CDR 데이터 를 파싱 하여, Hadoop FileSystem 에 LogRecordKey, LogRecord 의 Map 형태로 저장 한다.
 */
public class CdrDataParser {

    private final String _ENCODING = "EUC-KR";

    private final String _columnFilePath;
    private final String _dataFilePath;

    private ArrayList<String> _arrColumnData;
    private MapFileWriter _mapFileWriter;

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

    /**
     * Column 명이 정의 되어 있는 파일을 읽어, ArrayList<String> 타입 으로 저장 한다.
     * @throws IOException  컬럼 파일 이 없거나, 읽을 수 없을 경우에 에러가 난다.
     */
    private void loadColumnData() throws IOException {
        File file = new File(_columnFilePath);
        if(!file.isDirectory()) file.mkdir();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

        String row;
        while((row = reader.readLine()) != null){
            _arrColumnData.add(row.trim());
        }
    }

    /**
     * CDR Data 를 파싱 하여, File System 에 작성 한다.
     * @throws IOException  DataFile 을 읽을 수 없을 때, 에러가 발생 한다.
     */
    public void start() throws IOException {
        File file = new File(_dataFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

        String row;
        int dataCount = 0;
        while((row = reader.readLine()) != null){
            LogRecord logRecord = new LogRecord();

            String _SEPARATOR = ",";
            String[] cols = row.split(_SEPARATOR);

            for(int i = 0 ; i < cols.length ; i++) {
                logRecord.add(_arrColumnData.get(i), cols[i]);
            }

            for(int i = 0 ; i < _arrColumnData.size(); i++){
                String col;
                try {
                    col = cols[i];
                } catch(Exception e){
                    col = "";
                }
                if(col == null) col = "";

                logRecord.add(_arrColumnData.get(i), col);
            }

            LogRecordKey logRecordKey = new LogRecordKey();

            logRecordKey.setLogId(String.format("%09d", dataCount++));
            logRecordKey.setTime(String.valueOf(System.currentTimeMillis()));
            logRecordKey.setDataType("");

            _mapFileWriter.getMapFileWriter().append(logRecordKey, logRecord);
        }
    }

    public static void main(String[] args){
        String mapFilePath, columnFilePath, dataFilePath;
        if(args.length > 0){
            mapFilePath = args[0];
            columnFilePath = args[1];
            dataFilePath = args[2];
        } else {
            mapFilePath = "/home/david/Data/SearchPlatform/CDR/hdfs/";
            columnFilePath = "/home/david/Data/SearchPlatform/CDR/real_key.txt";
            dataFilePath = "/home/david/Data/SearchPlatform/CDR/data.dat";

        }

        try{
            CdrDataParser parser = new CdrDataParser(mapFilePath, columnFilePath, dataFilePath);
            parser.start();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}

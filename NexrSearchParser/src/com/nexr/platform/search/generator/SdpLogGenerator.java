package com.nexr.platform.search.generator;

import java.io.*;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/27/11
 * Time: 11:02 AM
 * SDP LOG DATA 를 Generator 한다.
 * TimeStamp 와 Transaction id 값을 Unique 하게 생성 한다.
 */
public class SdpLogGenerator {

    private final int _roofCount;
    private final String _saveFilePath;
    private final BufferedReader _xmlFileReader;
    private Map<String, String> _mapColumnData;

    private final String ENCODING = "utf-8";

    public SdpLogGenerator(String saveFilePath,String xmlFilePath, String columnFilePath, int roofCount) throws IOException {

        _saveFilePath = saveFilePath;
        _roofCount =roofCount;

        File file = new File(xmlFilePath);

        if(!file.exists()) {
            throw new IOException("Can't Find SDP xml File.");
        }

        _xmlFileReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), ENCODING));

        try {
            this.loadColumnFile(columnFilePath);
        } catch(IOException e){
            throw new IOException("Can't Find SDP Column Define File.");
        }
    }

    /**
     * Xml Data Node Name 을 컨버팅 한다.
     * File 의 형태는 [원본 노드명] "\t" [바뀔 노드명]이 된다.
     * Ex > TransactionLog.SdpHeader.test   TSL.SHD.test
     *
     * @param columnFilePath    Column Define File Path
     * @throws IOException      Column File Load Error
     */
    private void loadColumnFile(String columnFilePath) throws IOException {
        File file = new File(columnFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), ENCODING));
        String row;
        while((row = reader.readLine()) != null) {
            if(!row.isEmpty()){
                String _SPLIT = "\t";
                String[] cols = row.split(_SPLIT);
                if(cols.length > 1) _mapColumnData.put(cols[0], cols[1]);
            }
        }
    }

    public void start() throws IOException {
        String row;
        for(int i = 0; i < _roofCount; i++){
            while((row = _xmlFileReader.readLine()) != null){
                if(!row.isEmpty()) {
                    this.logGenerator(row);
                }
            }
        }
    }

    public String logGenerator(String xmlRow) {
        String rtnVal="";

        return rtnVal;
    }

    public void close(){
        try {
            _xmlFileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void main(String[] args){
        String saveFilePath, xmlFilePath, columnFilePath;
        int roofCount;


        if(args.length > 0) {

            saveFilePath = args[0];
            xmlFilePath = args[1];
            columnFilePath = args[2];
            roofCount = Integer.parseInt(args[3]);

        } else {
            saveFilePath = "/home/david/Data/SearchPlatform/SDP/hdfs/";
            xmlFilePath = "/home/david/Data/SearchPlatform/SDP/sdpXmlData.xml";
            columnFilePath = "/home/david/Data/SearchPlatform/SDP/SdpColumnDefine.txt";
            roofCount = 10;
        }


    }
}

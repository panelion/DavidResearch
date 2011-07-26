package com.nexr.search.platform.parser;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;

import com.nexr.search.platform.parser.io.AppendRootInputStream;
import com.nexr.search.platform.parser.io.MapFileWriter;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Xml 형태로 된, SDP Data 를 읽어와 Hadoop File System 형식의 LogRecordKey, LogRecord 의 Map 형태로 저장한다.
 * David.Woo - 2011.07.26
 */
public class SdpDataParser implements DataParser {

    private final String _SEPARATOR = ".";
    private final String _ENCODING = "UTF-8";
    private final String _SPLIT = "\t";

    private XMLEventReader _xmlEventReader;
    private MapFileWriter _mapFileWriter;
    private Map<String, String> _mapColumnData;

    /**
     * Constructor
     * @param mapFilePath   File System 을 저장 할 경로 ( 디렉 토리 )
     * @param xmlFilePath   Xml File 경로
     * @param columnFilePath
     */
    public SdpDataParser(String mapFilePath, String xmlFilePath, String columnFilePath){

        File mapFile = new File(mapFilePath);
        if(!mapFile.isDirectory()) mapFile.mkdirs();

        _mapFileWriter = new MapFileWriter(mapFilePath);
        _mapColumnData = new HashMap<String, String>();

        try {
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            _xmlEventReader = xmlInputFactory.createXMLEventReader(AppendRootInputStream.createInputStream(xmlFilePath, "root"));

            _mapFileWriter.open();
            this.loadColumnFile(columnFilePath);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Xml Data Node Name 을 컨버팅 한다.
     * File 의 형태는 [원본 노드명] "\t" [바뀔 노드명]이 된다.
     * Ex > TransactionLog.SdpHeader.test   TSL.SHD.test
     * @param columnFilePath
     * @throws IOException
     */
    private void loadColumnFile(String columnFilePath) throws IOException {
        File file = new File(columnFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
        String row;
        while((row = reader.readLine()) != null) {
            if(!row.isEmpty()){
                String[] cols = row.split(_SPLIT);
                if(cols.length > 1) _mapColumnData.put(cols[0], cols[1]);
            }
        }
    }

    /**
     * 시작
     */
    public void start() {

        LogRecord record = new LogRecord();

        int i = 0;

        String firstKey = "TransactionLog";

        try {
            while(this._xmlEventReader.hasNext()){

                XMLEvent event = this._xmlEventReader.nextEvent();

                if(this.isStartNode(event, firstKey)){
                    record = new LogRecord();
                    continue;
                }

                for(COLUMN_LIST COLUMN : COLUMN_LIST.values()){
                    if(this.isStartNode(event, COLUMN.name())){
                        String columnName = COLUMN.name();
                        String keyValue = firstKey + _SEPARATOR + columnName;
                        record = this.parseToAttribute(record, keyValue, columnName);
                    }
                }

                if(this.isEndNode(event, firstKey)){
                    LogRecordKey logRecordKey = new LogRecordKey();

                    logRecordKey.setLogId(String.format("%09d", i++));
                    //System.out.println(i++);
                    // logRecordKey.setLogId(UUID.randomUUID().toString());
                    logRecordKey.setTime(String.valueOf(System.currentTimeMillis()));
                    logRecordKey.setDataType("");

                    _mapFileWriter.getMapFileWriter().append(logRecordKey, record);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 컬럼의 각 부분 대표 명칭에 대한 Enum
     */
    public enum COLUMN_LIST{
        SystemHeader,
        DataHeader,
        Body
    }

    /**
     * 종료
     * @throws XMLStreamException
     * @throws IOException
     */
    public void close() throws XMLStreamException, IOException {
        _xmlEventReader.close();
        _mapFileWriter.close();
    }

    /**
     * Xml Data 의 시작 노드 인지를 분별 한다.
     * @param event
     * @param columnName
     * @return
     * @throws XMLStreamException
     */
    public boolean isStartNode(XMLEvent event, String columnName) throws XMLStreamException {
        if(event.isStartElement()){
           if(event.asStartElement().getName().getLocalPart().equals(columnName)) {
               return true;
           }
        }
        return false;
    }

    /**
     * Xml Data 의 노드의 끝 부분 인지 분별 한다.
     * @param event
     * @param columnName
     * @return
     * @throws XMLStreamException
     */
    public boolean isEndNode(XMLEvent event, String columnName) throws XMLStreamException {
        if(event.isEndElement()){
           if(event.asEndElement().getName().getLocalPart().equals(columnName)) {
               return true;
           }
        }
        return false;
    }

    /**
     * xml Data 를 Parsing 하여, 하위 노드의 Attribute 의 값을 LogRecord 형태로 리턴 한다.
     * @param logRecord
     * @param keyValue
     * @param closedColumnName
     * @return
     * @throws XMLStreamException
     */
    public LogRecord parseToAttribute(LogRecord logRecord, String keyValue, String closedColumnName) throws XMLStreamException {

        boolean parseBool = true;

        String key = keyValue + _SEPARATOR;
        String value = "";

        while(parseBool){

            XMLEvent event = _xmlEventReader.nextEvent();

            if(event.isStartElement()){
                key += event.asStartElement().getName().getLocalPart();
            } else if(event.isCharacters()) {
                value = event.asCharacters().getData();
            } else if(event.isEndElement()){
                if(event.asEndElement().getName().getLocalPart().equals(closedColumnName)){
                    parseBool = false;
                } else {
                    /**
                     * TODO : 이 부분 에서, key 값을 Converting 한다.
                     */
                    if(_mapColumnData.containsKey(key)) {
                        String temp_key = _mapColumnData.get(key);
                        key = temp_key;
                        logRecord.add(key, value);
                    } else {
                        System.out.println("[ERROR CODE] : " + key + " + " + value);
                    }

                    key = keyValue + _SEPARATOR;
                    value = "";
                }
            }
        }
        return logRecord;
    }

    public static void main(String[] args) throws XMLStreamException, IOException {

        String mapFilePath, xmlFilePath, columnFilePath;
        if(args.length > 0) {

            mapFilePath = args[0];
            xmlFilePath = args[1];
            columnFilePath = args[2];

        } else {
            mapFilePath = "/home/david/Data/SearchPlatform/SDP/hdfs/";
            xmlFilePath = "/home/david/Data/SearchPlatform/SDP/sdpXmlData.xml";
            columnFilePath = "/home/david/Data/SearchPlatform/SDP/SdpColumnDefine.txt";
        }

        SdpDataParser xmlParser = new SdpDataParser(mapFilePath, xmlFilePath, columnFilePath);
        xmlParser.start();
        xmlParser.close();
    }
}

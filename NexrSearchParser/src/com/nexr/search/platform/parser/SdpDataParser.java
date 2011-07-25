package com.nexr.search.platform.parser;

import com.nexr.data.sdp.rolling.hdfs.LogRecord;
import com.nexr.data.sdp.rolling.hdfs.LogRecordKey;

import com.nexr.search.platform.parser.io.AppendRootInputStream;
import com.nexr.search.platform.parser.io.MapFileWriter;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.FileNotFoundException;
import java.io.IOException;

public class SdpDataParser {

    private String _xmlFilePath;

    private XMLEventReader _xmlEventReader;

    public void setXmlFilePath(String xmlFilePath){
        this._xmlFilePath = xmlFilePath;
    }

    public XMLEventReader getXmlEventReader(){
        return this._xmlEventReader;
    }

    public void open() throws FileNotFoundException, XMLStreamException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        _xmlEventReader = xmlInputFactory.createXMLEventReader(AppendRootInputStream.createInputStream(_xmlFilePath, "root"));
    }
    public void close() throws XMLStreamException {
        _xmlEventReader.close();
    }

    public boolean isStartNode(XMLEvent event, String columnName) throws XMLStreamException {
        if(event.isStartElement()){
           if(event.asStartElement().getName().getLocalPart().equals(columnName)) {
               return true;
           }
        }
        return false;
    }

    public boolean isEndNode(XMLEvent event, String columnName) throws XMLStreamException {
        if(event.isEndElement()){
           if(event.asEndElement().getName().getLocalPart().equals(columnName)) {
               return true;
           }
        }
        return false;
    }

    public LogRecord parseToAttribute(XMLEventReader xmlEventReader, LogRecord logRecord, String keyValue, String secondColumnName) throws XMLStreamException {

        boolean parseBool = true;

        String key = keyValue + ".";
        String value = "";

        while(parseBool){

            XMLEvent event = xmlEventReader.nextEvent();

            if(event.isStartElement()){
                key += event.asStartElement().getName().getLocalPart();
            } else if(event.isCharacters()) {
                value = event.asCharacters().getData();
            } else if(event.isEndElement()){
                if(event.asEndElement().getName().getLocalPart().equals(secondColumnName)){
                    parseBool = false;
                } else {
                    logRecord.add(key, value);
                    key = keyValue + ".";
                    value = "";
                }
            }
        }
        return logRecord;
    }

    public static void main(String[] args) throws XMLStreamException, IOException {

        String mapFilePath, xmlFilePath;
        if(args.length > 0) {

            mapFilePath = args[0];
            xmlFilePath = args[1];

        } else {
            mapFilePath = "/home/david/Data/SearchPlatform/SDP/hdfs/";
            xmlFilePath = "/home/david/Data/SearchPlatform/SDP/sdpXmlData.xml";
        }

        SdpDataParser xmlParser = new SdpDataParser();
        MapFileWriter mapFileWriter = new MapFileWriter(mapFilePath);

        xmlParser.setXmlFilePath(xmlFilePath);

        xmlParser.open();
        mapFileWriter.open();

        LogRecord record = new LogRecord();

        int i = 0;

        String firstKey = "TransactionLog";

        while(xmlParser.getXmlEventReader().hasNext()){

            XMLEvent event = xmlParser.getXmlEventReader().nextEvent();

            if(xmlParser.isStartNode(event, firstKey)){
                record = new LogRecord();
                continue;
            }

            for(COLUMN_LIST COLUMN : COLUMN_LIST.values()){
                if(xmlParser.isStartNode(event, COLUMN.name())){
                    String columnName = COLUMN.name();
                    String keyValue = firstKey + "." + columnName;
                    record = xmlParser.parseToAttribute(xmlParser.getXmlEventReader(), record, keyValue, columnName);
                }
            }

            if(xmlParser.isEndNode(event, firstKey)){
                LogRecordKey logRecordKey = new LogRecordKey();

                logRecordKey.setLogId(String.format("%09d", i++));
                //System.out.println(i++);
                // logRecordKey.setLogId(UUID.randomUUID().toString());
                logRecordKey.setTime(String.valueOf(System.currentTimeMillis()));
                logRecordKey.setDataType("");

                mapFileWriter.getMapFileWriter().append(logRecordKey, record);
            }
        }

        xmlParser.close();
        mapFileWriter.close();

    }

    public enum COLUMN_LIST{
        SystemHeader,
        DataHeader,
        Body
    }

}

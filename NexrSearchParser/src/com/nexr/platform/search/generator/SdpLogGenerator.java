package com.nexr.platform.search.generator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/27/11
 * Time: 11:02 AM
 * SDP LOG DATA 를 Generator 한다.
 * TimeStamp 와 Transaction id 값을 Unique 하게 생성 한다.
 *
 * Original Xml Data : 798,912 rows.
 */
public class SdpLogGenerator extends AbstractSdpLogGenerator {

    private Map<String, String> _mapColumnData;
    private Transformer _trans;
    private BufferedWriter _logWriter;
    private final String _ip;
    private final String _currentTime;
    private int _logCount;

    private Date _date;
    private Calendar _cal;
    private SimpleDateFormat _simpleDateFormat;

    private final String ENCODING = "utf-8";
    private final String SEPARATOR = ".";


    public int getLogCount() {
        return _logCount;
    }

    public SdpLogGenerator(String saveFilePath, String columnFilePath, String ip) throws IOException, XMLStreamException {
        /**
         *  Column Define Data 정보를 불러와 Map 형태로 저장 한다.
         */
        try {
            _mapColumnData = new HashMap<String, String>();
            this.loadColumnFile(columnFilePath);
        } catch(IOException e){
            throw new IOException("Can't Find SDP Column Define File.");
        }

        /**
         * 저장 할 파일을 새로 생성 한다.
         */
        try {
            TransformerFactory _transFactory = TransformerFactory.newInstance();
            _trans = _transFactory.newTransformer();

            File saveFile = new File(saveFilePath);
            saveFile.createNewFile();

            FileWriter fw = new FileWriter(saveFile);
            _logWriter = new BufferedWriter(fw);

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        }

        /**
         * IP 를 변환 한다.
         */
        ip = ip.replaceAll("//.", "//-");
        _ip = ip;

        /**
         * 현재 시간을 생성 한다.
         */
        _simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        _date = new Date();
        _currentTime = _simpleDateFormat.format(_date);

        /**
         * 한달 전의 날짜를 구한다.
         */
        _simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        _cal = Calendar.getInstance();
        _cal.setTime(_date);
        _cal.add(Calendar.MONTH , - 1);
    }

    private String getAddSecondDate(){
        _cal.add(Calendar.SECOND , 1);
        _date = _cal.getTime();
        return _simpleDateFormat.format(_date);
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

    /**
     * Xml Log Data 를 생성 한다.
     *
     * @param xmlFilePath   Xml Data 가 저장 되어 있는 파일 경로
     * @param roofCount     반복 해서 읽을 횟수.
     *
     * @throws IOException  Xml Data File 이 없을 경우 발생
     */
    public void start(String xmlFilePath, int roofCount) throws IOException {
        String row;

        File file = new File(xmlFilePath);

        if(!file.exists()) {
            throw new IOException("Can't Find SDP xml File.");
        }

        for(int i = 0; i < roofCount; i++){

            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), ENCODING);
            BufferedReader xmlFileReader = new BufferedReader(inputStreamReader);

            while((row = xmlFileReader.readLine()) != null){
                // System.out.println(row);
                if(!row.isEmpty()) {
                    try {
                        this.logGenerator(row);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("start new reload");

            xmlFileReader.close();
            inputStreamReader.close();

            row = null;
        }
    }

    /**
     * 기존의 Xml Log Data 를 이용 하여, 새로운 Log Data 를 생성 한다.
     * @param xmlRow    String Xml Row Data;
     */
    private void logGenerator(String xmlRow) {

        Element readElement = this.getElementByStrXml(xmlRow);

        if(readElement == null) {
            return;
        }

        if(readElement.hasChildNodes()) {

            String firstNodeName = readElement.getNodeName(), secondNodeName,
                    thirdNodeName, tempFirstNodeName = "", newNodeName;

            String[] newNodeNames;

            Document writeDoc = this.createDocument();

            Element rootElement = null, secondElement;
            NodeList nodeList = readElement.getChildNodes();

            Node node, childNode;
            ArrayList<Element> thirdElementList;

            NodeList childNodeList;

            for(int i = 0 ; i < nodeList.getLength(); i++) {

                node = nodeList.item(i);

                if(node.hasChildNodes()){

                    secondNodeName = node.getNodeName();
                    childNodeList = node.getChildNodes();

                    thirdElementList = new ArrayList<Element>();

                    for(int j = 0; j < childNodeList.getLength(); j++){

                        childNode = childNodeList.item(j);

                        if(childNode.getNodeType() == Node.ELEMENT_NODE) {

                            thirdNodeName = childNode.getNodeName();

                            String textValue = childNode.getTextContent();
                            // textValue = new String(textValue .getBytes("8859_1"), "UTF-8");

                            String orgNodeName = firstNodeName + SEPARATOR + secondNodeName + SEPARATOR + thirdNodeName;

                            if(_mapColumnData.containsKey(orgNodeName)) {
                                newNodeName = _mapColumnData.get(orgNodeName);
                                newNodeNames = newNodeName.split("\\" + SEPARATOR);
                                if(newNodeNames.length > 0) {
                                    newNodeName = newNodeNames[newNodeNames.length - 1];
                                    if(!newNodeName.isEmpty()) {
                                        thirdElementList.add(this.makeNewValue(writeDoc, newNodeName, textValue));
                                        if(childNodeList.getLength() - 1 == j) {
                                            secondNodeName = newNodeNames[1];
                                            tempFirstNodeName = newNodeNames[0];
                                        }
                                    }
                                }
                            }
                        }
                    }

                    secondElement = writeDoc.createElement(secondNodeName);

                    for(Element e : thirdElementList) {
                        secondElement.appendChild(e);
                    }

                    if(i == 0 && rootElement == null && !tempFirstNodeName.isEmpty()) {
                        rootElement = writeDoc.createElement(tempFirstNodeName);
                    }

                    rootElement.appendChild(secondElement);
                }
            }

            try {
                this.save(rootElement);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 기존의 Element Value 를 버리고, 새로운 Element Value 를 customizing 한다.
     * @param document  Document
     * @param key       키 값
     * @param value     Value
     * @return          Element
     */
    private Element makeNewValue(Document document, String key, String value) {

        Element element = document.createElement(key);
        if(key.equals("PL")) {
            element.appendChild(document.createCDATASection(value));
        } else if(key.equals("TXID")) {
            // ip + currentTimeStamp + "-" + "번호".
            // 0308c137-543e-3ea8-b929-f699d4a00864
            // UUID.randomUUID().toString(); or currentTimeStamp + 번호?
            element.setTextContent(_ip + "-" + _currentTime + "-" + String.format("%09d", _logCount++));

        } else if(key.equals("TS")){
            // 2011-06-22 02:57:37.118
            element.setTextContent(this.getAddSecondDate());

        } else {
            element.setTextContent(value);
        }
        return element;
    }


    /**
     * 변환 된 Xml Data 를 File 에 저장 한다.
     * @param element   Xml Data Element.
     */
    private void save(Element element) {
        try {
            StringWriter buffer = new StringWriter();

            _trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            _trans.transform(new DOMSource(element), new StreamResult(buffer));

            _logWriter.write(buffer.toString());
            _logWriter.write(System.getProperty("line.separator"));

            buffer = null;

            _logWriter.flush();

            if(_logCount % 10000 == 0)
                System.out.println(_logCount);

        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(int count) {

    }

    /**
     * 모든 process 를 닫는다.
     */
    public void close(){
        try {
            _logWriter.flush();
            _logWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws XMLStreamException, IOException {
        String saveFilePath, xmlFilePath, columnFilePath, ip;
        int roofCount;


        if(args.length > 0) {

            saveFilePath = args[0];
            xmlFilePath = args[1];
            columnFilePath = args[2];
            roofCount = Integer.parseInt(args[3]);

            ip = args[4];

        } else {
            saveFilePath = "/Users/panelion/Data/SdpGenerateData.log";
            xmlFilePath = "/Users/panelion/Data/sdpParseData.log";
            columnFilePath = "/Users/panelion/Data/SdpColumnDefine.txt";
            roofCount = 10;
            ip = "10.1.1.1";
        }

        SdpLogGenerator sdpLogGenerator = new SdpLogGenerator(saveFilePath, columnFilePath, ip);
        System.out.println("[Start Log Generator]");
        sdpLogGenerator.start(xmlFilePath, roofCount);
        sdpLogGenerator.close();
        System.out.println("TOTAL ROW COUNT : " + sdpLogGenerator.getLogCount());
        System.out.println("[End Log Generator]");

    }
}

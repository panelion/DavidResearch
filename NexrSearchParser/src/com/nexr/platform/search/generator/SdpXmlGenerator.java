package com.nexr.platform.search.generator;


import com.nexr.platform.search.entity.sdp.BodyEntity;
import com.nexr.platform.search.entity.sdp.DataHeaderEntity;
import com.nexr.platform.search.entity.sdp.SDPLogEntity;
import com.nexr.platform.search.entity.sdp.SystemHeaderEntity;
import com.nexr.platform.search.utils.DateUtils;
import com.nexr.platform.search.utils.FileUtils;
import com.nexr.platform.search.utils.XmlUtils;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Random;

public class SdpXmlGenerator {

    private Random rnd;

    private final String CID = "KT";
    private final String SYSID = "SDP";
    private final String LT = "Transaction";
    private final String prefixUserId = "USER";
    private final String prefixScreenId = "SCREEN";

    private final String[] objectNames = {
            "RetrievePartyProfile",
            "IdAndPasswordAuthentication",
            "CheckOllehIDExistForQookInternet",
            "RetrieveCredentialsByUsername",
            "RetrieveServiceDomains",
            "RetrieveProductList",
            "RetrievePartyRealNameCheck",
            "RetrieveQookAndShowInfo",
            "UnregisterProduct",
            "RegisterProduct"
    };

    private final String[] methodNames = {
            "handleRequest",
            "executeRetrieveServiceDomains",
            "executeCheckOllehMembership",
            "executeRetrieveCredentials"
    };

    private final String moduleType;
    private final String serverName;
    private final String serverIP;
    private final String logRecordType;

    private final int idMaxCount;

    private String prefixTxLog;

    private final int startSeqNum = 1;
    private final int endSeqNum = 9999;

    public SdpXmlGenerator(int idMaxCount, String moduleType, String serverName, String serverIP, String logRecordType) {

        rnd = new Random();

        this.idMaxCount = idMaxCount;
        this.serverName = serverName;
        this.serverIP = serverIP;
        this.moduleType = moduleType;

        this.logRecordType = logRecordType;
        this.prefixTxLog = serverIP.replaceAll("\\.", "\\-");
    }

    public void generate(String saveFilePath, int xmlRowCount) throws ParserConfigurationException, IOException {

        XmlUtils xmlUtils = new XmlUtils();
        FileUtils fileUtils = new FileUtils(saveFilePath);
        DateUtils dateUtils = new DateUtils("yyyyMMddHHmmss");

        fileUtils.createNewFile();
        fileUtils.setWritable(true);

        prefixTxLog = prefixTxLog + "-" + dateUtils.getCurrentTime() + "-";
        dateUtils.setDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        SDPLogEntity entity = new SDPLogEntity(xmlUtils.getDocument());
        dateUtils.getAddMonth(-1);

        int txNum = 1;

        for(int i = 1 ; i <= xmlRowCount; i++) {

            /**
             * System Header 부분 생성.
             */
            this.makeSystemHeaderData(entity.getSystemHeaderEntity(), i);

            /**
             * Data Header 부분 생성.
             */
            String TXID = prefixTxLog + String.format("%09d", txNum);
            String objectName = this.objectNames[i % objectNames.length];
            String methodName = this.methodNames[i % methodNames.length];
            String strPayLoad = entity.generateCData(methodName, objectName, TXID);

            this.makeDataHeaderData(entity.getDataHeaderEntity(), i, TXID, dateUtils.getAddSecond(1), objectName, methodName, strPayLoad.length());

            if(i % this.idMaxCount == 0) txNum++;

            /**
             * Body 부분 생성.
             */
            this.makeBodyData(entity.getBodyEntity(), i, strPayLoad);

            /**
             * 파일에 기록 하기.
             */
            fileUtils.writeLine(entity.toString());

            if(i % 10000 == 0) {
                System.out.println(i);
            }
        }

        fileUtils.close();
        xmlUtils.close();
    }

    private void makeSystemHeaderData(SystemHeaderEntity entity, int totalCount) {
        entity.setCID(CID);
        entity.setSysId(SYSID);
        entity.setLT(LT);
        entity.setUID(prefixUserId + String.format("%04d", totalCount % idMaxCount));
        entity.setScId(prefixScreenId + String.format("%04d", totalCount % idMaxCount));
    }

    private void makeDataHeaderData(DataHeaderEntity entity, int totalCount, String TXID, String strTimeStamp, String objectName, String methodName, int payLoadSize) {

        entity.setTxId(TXID);

        if(totalCount % idMaxCount == 1) {
            entity.setSeq(String.valueOf(startSeqNum));
        } else if (totalCount % idMaxCount == 0) {
            entity.setSeq(String.valueOf(endSeqNum));
        } else {
            entity.setSeq(String.valueOf(rnd.nextInt(endSeqNum - idMaxCount) + (idMaxCount - startSeqNum)));
        }

        entity.setTS(strTimeStamp);
        entity.setMT(this.moduleType);
        entity.setSN(this.serverName);
        entity.setSIP(this.serverIP);
        entity.setON(objectName);
        entity.setMN(methodName);
        entity.setPLS(String.valueOf(payLoadSize));
    }

    private void makeBodyData(BodyEntity entity, int totalCount, String payLoad) {
        entity.setLTP(this.logRecordType);
        if(this.logRecordType.toUpperCase().equals("IN_RES") || this.logRecordType.toUpperCase().equals("OUT_RES")) {
            String codeVal = String.valueOf(totalCount % 2);
            entity.setRC(codeVal);
            if(codeVal.equals("0")) entity.setEC(codeVal);
            else entity.setEC(" ");
        }

        entity.setRD(" ");
        entity.setED(" ");
        entity.setPL(payLoad);
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException {

        int idMaxCount, xmlRowCount;
        String moduleType, serverName, serverIP, logRecordType, saveFilePath;

        if(args.length > 0) {

            idMaxCount = Integer.parseInt(args[0]);
            xmlRowCount = Integer.parseInt(args[1]);

            moduleType = args[2];
            serverName = args[3];
            serverIP = args[4];
            logRecordType = args[5];
            saveFilePath = args[6];

        } else {
            idMaxCount = 8;
            xmlRowCount = 11;

            // WAMUI/CSMUI, OCSG, SO, CSM, etc
            moduleType = "CSM";

            serverName = "david Server";
            serverIP = "127.0.0.1";

            // IN_RES, OUT_RES, IN_REQ, OUT_REQ
            logRecordType = "IN_RES";
            saveFilePath = "/home/david/Data/SearchPlatform/SDP/generateParseData.log";
        }

        long startTime = System.currentTimeMillis();
        SdpXmlGenerator sdpXmlGenerator = new SdpXmlGenerator(idMaxCount, moduleType, serverName, serverIP, logRecordType);
        sdpXmlGenerator.generate(saveFilePath, xmlRowCount);

        long endTime = System.currentTimeMillis();

        System.out.println("TOTAL SPEND TIME :::::: " + (endTime - startTime));


    }
}

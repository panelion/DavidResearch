package com.nexr.platform.search.generator;


import com.nexr.platform.search.entity.sdp.BodyEntity;
import com.nexr.platform.search.entity.sdp.DataHeaderEntity;
import com.nexr.platform.search.entity.sdp.SDPLogEntity;
import com.nexr.platform.search.entity.sdp.SystemHeaderEntity;
import com.nexr.platform.search.generator.transaction.Transaction;
import com.nexr.platform.search.generator.transaction.TransactionSequence;
import com.nexr.platform.search.utils.XmlUtils;
import com.nexr.platform.search.utils.FileUtils;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.*;import java.lang.Integer;import java.lang.Long;import java.lang.Math;import java.lang.String;import java.lang.System;import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;import java.util.ArrayList;import java.util.Arrays;import java.util.Date;import java.util.List;import java.util.Random;
import org.apache.log4j.Logger;

public class SdpXmlGenerator2 {

    private Random rnd;

    private final int idMaxCount;
    private final int seqMaxCount;
    private final long maxTransTime;
    private final long maxTransInterval;
    private final long startTime;
    private String prefixTxLog;

    private final int startSeqNum = 1;
    private final int endSeqNum = 9999;

    private Logger logger = Logger.getLogger(SdpXmlGenerator2.class);

    private static final boolean IS_DEBUG = true;


    public SdpXmlGenerator2(int idMaxCount, int seqMaxCount, long startTime, long maxTransTime, long maxTransInterval) {
        this.idMaxCount = idMaxCount;
        this.seqMaxCount = seqMaxCount;
        this.startTime = startTime;
        this.prefixTxLog = FieldData.serverIPs[0].replaceAll("\\.", "\\-");
        this.maxTransTime = maxTransTime;
        this.maxTransInterval = maxTransInterval;
    }

    public void generate(String saveFilePath, int transactionCount, String moduleType, String serverName, String serverIP, String logType) throws ParserConfigurationException, IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        prefixTxLog = prefixTxLog + "-" + dateFormat.format(new Date(System.currentTimeMillis())) + "-";

        long lastWriteTime = startTime;
        long currentStartTime = startTime;

        List<Transaction> transList = new ArrayList<Transaction>();
        Random rnd = new Random(System.currentTimeMillis());

        TransactionWriter transWriter;
        transWriter = new TransactionWriter(moduleType, serverName, serverIP, logType, saveFilePath);
        transWriter.open();

        for (int transNum=0; transNum<transactionCount; transNum++) {
            String TXID = prefixTxLog + String.format("%09d", transNum);
            String objectName = FieldData.objectNames[transNum % FieldData.objectNames.length];
            String methodName = FieldData.methodNames[transNum % FieldData.methodNames.length];

            Transaction transaction = new Transaction(Math.abs(rnd.nextInt()%idMaxCount), Math.abs(rnd.nextInt()%3), TXID, objectName, methodName,
                    currentStartTime, maxTransTime, this.seqMaxCount, IS_DEBUG);
            transList.add(transaction);

            currentStartTime += Math.abs(rnd.nextLong() % maxTransInterval);
            if (currentStartTime - lastWriteTime > 1000*60) {
                transWriter.write(transList, currentStartTime);
                lastWriteTime = currentStartTime;
            }
        }

        transWriter.write(transList, Long.MAX_VALUE);
        transWriter.close();
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, ParseException {
        int userCount, seqMaxCount, transactionCount;
        String dateString;
        String saveFilePath;
        String moduleType;
        String serverName;
        String serverIP;
        String logType;
        long logStartTime;
        long maxTransTime;
        long maxTransInterval;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        Logger logger = Logger.getLogger(SdpXmlGenerator2.class);

        if(args.length > 0) {

            userCount = java.lang.Integer.parseInt(args[0]);
            seqMaxCount = Integer.parseInt(args[1]);
            transactionCount = Integer.parseInt(args[2]);
            dateString = args[3];
            maxTransTime = Long.parseLong(args[4]);
            maxTransInterval = Long.parseLong(args[5]);
            moduleType = args[6];
            serverName = args[7];
            serverIP = args[8];
            logType = args[9];

            saveFilePath = args[10];

        } else {
            userCount = 100;
            seqMaxCount = 8;
            // transaction 당 10k 생성
            // 20000 transaction 으로 시험시 14 시간 가량 데이터 생성
            // 14*60*60*1000 = 50400000
            transactionCount = 3;
            dateString = "2011-08-01";
            maxTransTime = 60000;
            // 시험상으로 transactionCount*maxTransInterval/2 정도의 기간으로 transaction 이 생성
            maxTransInterval = 5000;
            moduleType = FieldData.moduleTypes[0];
            serverName = FieldData.serverNames[0];
            serverIP = FieldData.serverIPs[0];
            logType = FieldData.logTypes[0];
            saveFilePath = "/data/SDP/generateParseData.log";
        }

        dateString = dateString.trim() + " 00:00:00.000";
        logStartTime =  dateFormat.parse(dateString).getTime();

        long startTime = System.currentTimeMillis();

        logger.info("**********************************************************");
        logger.info("Log Generator START");
        logger.info("**********************************************************");
        logger.info("Configurations:");
        logger.info("- USER COUNT           : " + userCount);
        logger.info("- SEQ/TRANSACTION      : " + seqMaxCount);
        logger.info("- TRANSACTION COUNT    : " + transactionCount);
        logger.info("- START TIME           : " + dateString);
        logger.info("- MAX TRANSACTION TIME : " + maxTransTime);
        logger.info("- TRANSACTION INTERVAL : " + maxTransInterval);
        logger.info("- SAVE FILE PATH       : " + saveFilePath);
        logger.info("");
        logger.info("* Start generating log...");

        SdpXmlGenerator2 sdpXmlGenerator = new SdpXmlGenerator2(userCount, seqMaxCount, logStartTime, maxTransTime, maxTransInterval);
        sdpXmlGenerator.generate(saveFilePath, transactionCount, moduleType, serverName, serverIP, logType);

        long endTime = System.currentTimeMillis();
        logger.info("..finished.");
        logger.info("");
        logger.info("TOTAL SPEND TIME :::::: " + (endTime - startTime));


    }

    public static class TransactionWriter {
        private FileUtils fileUtil;
        private XmlUtils xmlUtils;

        private String moduleType;
        private String serverName;
        private String serverIP;
        private String logType;

        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        public TransactionWriter(String moduleType, String serverName, String serverIP, String logType, String logPath) throws IOException, ParserConfigurationException {
            this.moduleType = moduleType;
            this.serverName = serverName;
            this.serverIP = serverIP;
            this.logType = logType;

            fileUtil = new FileUtils(logPath);
            xmlUtils = new XmlUtils();
        }

        public void open() throws IOException {
            fileUtil.createNewFile();
            fileUtil.setWritable(true);
        }
        public void close() throws IOException {
            fileUtil.close();
            xmlUtils.close();
        }

        public void write(List<Transaction> transList, long time) throws IOException {

            List<TransactionSequence> sequenceList = getWritableSequence(transList, time);
            SDPLogEntity entity = new SDPLogEntity(xmlUtils.getDocument());

            TransactionSequence[] sequenceArray = sequenceList.toArray(new TransactionSequence[sequenceList.size()]);
            Arrays.sort(sequenceArray);

            for (TransactionSequence eachSequence : sequenceArray) {
                Transaction seqTransaction = eachSequence.getTransaction();

                makeSystemHeaderData(entity.getSystemHeaderEntity(), eachSequence);
                makeDataHeaderData(entity.getDataHeaderEntity(), eachSequence);
                makeBodyData(entity.getBodyEntity(), eachSequence);

                fileUtil.writeLine(entity.toString());
            }
        }

        private List<TransactionSequence> getWritableSequence(List<Transaction> transList, long time) {
            List<TransactionSequence> writable = new ArrayList<TransactionSequence>();
            List<Transaction> removeTransList = new ArrayList<Transaction>();

            for (Transaction eachTrans : transList) {
                List<TransactionSequence> sequenceList = eachTrans.getSequenceList();
                for (TransactionSequence eachSequence : sequenceList) {
                    if (eachSequence.getTimestamp() < time && eachSequence.getUsed() == false) {
                        writable.add(eachSequence);
                        eachSequence.setUsed(true);
                    }
                }

                if (eachTrans.isRemovable()) {
                    removeTransList.add(eachTrans);
                }
            }

            for (Transaction eachTrans : removeTransList) {
                transList.remove(eachTrans);
            }

            return writable;
        }

        private void makeSystemHeaderData(SystemHeaderEntity entity, TransactionSequence sequence) {
            entity.setCID(FieldData.CID);
            entity.setSysId(FieldData.SYSID);
            entity.setLT(FieldData.LT);
            entity.setUID(FieldData.prefixUserId + String.format("%04d", sequence.getTransaction().getUserId()));
            entity.setScId(FieldData.prefixScreenId + String.format("%04d", sequence.getTransaction().getScreenId()));
        }

        //private void makeDataHeaderData(DataHeaderEntity entity, int totalCount, String TXID, String strTimeStamp, String objectName, String methodName, int payLoadSize) {
        private void makeDataHeaderData(DataHeaderEntity entity, TransactionSequence sequence) {
            Transaction seqTransaction = sequence.getTransaction();

            entity.setTxId(seqTransaction.getTransId());
            entity.setSeq(sequence.getSeqNumber());
            entity.setTS(dateFormat.format(sequence.getTimestamp()));

            // temporary, use index 0
            entity.setMT(this.moduleType);
            entity.setSN(this.serverName);
            entity.setSIP(this.serverIP);

            entity.setON(seqTransaction.getObjectName());
            entity.setMN(seqTransaction.getMethodName());
            entity.setPLS(String.valueOf(seqTransaction.getPayload().length()));
        }

        private void makeBodyData(BodyEntity entity, TransactionSequence sequence) {
            entity.setLTP(this.logType);
            if(logType.toUpperCase().equals("IN_RES") || logType.toUpperCase().equals("OUT_RES")) {
                String codeVal = String.valueOf(sequence.getTransaction().getScreenId() % 2);
                entity.setRC(codeVal);
                if(codeVal.equals("0")) entity.setEC(codeVal);
                else entity.setEC(" ");
            }

            entity.setRD(" ");
            entity.setED(" ");
            entity.setPL(sequence.getTransaction().getPayload());
        }
    }

    private static class FieldData {
        private static final String CID = "KT";
        private static final String SYSID = "SDP";
        private static final String LT = "Transaction";
        private static final String prefixUserId = "USER";
        private static final String prefixScreenId = "SCREEN";

        private static final String[] objectNames = {
                "RetrievePartyProfile",
                "IdAndPasswordAuthentication",
                "CheckOllehIDExistForQookInternet",
                "RetrieveCredentialsByUsername",
                "RetrieveServiceDomains",
                "RetrieveProductList",
                "RetrievePartyReal1NameCheck",
                "RetrieveQookAndShowInfo",
                "UnregisterProduct",
                "RegisterProduct"
        };

        private static final String[] methodNames = {
                "handleRequest",
                "executeRetrieveServiceDomains",
                "executeCheckOllehMembership",
                "executeRetrieveCredentials"
        };

        private static final String[] serverNames = {
                "david Server"
        };

        private static final String[] moduleTypes = {
                "WAMUI",
                "CSMUI",
                "OCSG",
                "SO",
                "CSM"
        };

        private static final String[] serverIPs = {
                "127.0.0.1"
        };

        private static final String[] logTypes = {
                "IN_REQ",
                "OUT_REQ",
                "IN_RES",
                "OUT_RES"
        };
    }
}

package com.nexr.platform.search.generator.transaction;

import com.nexr.platform.search.entity.sdp.BaseClass;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: hcnah
 * Date: 11. 8. 9.
 * Time: 오후 4:29
 * To change this template use File | Settings | File Templates.
 */
public class Transaction {
    private Logger logger = Logger.getLogger(Transaction.class);

    private static final int START_SEQ_NUM = 1;
    private static final int END_SEQ_NUM = 9999;

    private int userId;
    private int screenId;
    private String transId;
    private String objectName;
    private String methodName;

    private long startTime;
    private long endTime;

    private String payload;

    private List<TransactionSequence> sequenceList;
    private static final Random random = new Random(System.currentTimeMillis());

    public Transaction(int userId, int screenId, String txID, String oName, String mName, long startTime, long maxTransTime, int seqCount, boolean isDebug) {
        this.userId = userId;
        this.screenId = screenId;
        this.transId = txID;
        this.objectName = oName;
        this.methodName = mName;
        this.startTime = startTime;

        this.payload = BaseClass.generateCData(mName, oName, txID);

        generateSequence(maxTransTime, seqCount, isDebug);
    }

    public int getUserId() {
        return userId;
    }
    public int getScreenId() {
        return screenId;
    }
    public String getTransId() {
        return transId;
    }
    public String getObjectName() {
        return objectName;
    }
    public String getMethodName() {
        return methodName;
    }
    public String getPayload() {
        return payload;
    }

    public List<TransactionSequence> getSequenceList() {
        return sequenceList;
    }

    private void generateSequence(long maxTransTime, int seqCount, boolean isDebug) {
        long[] intervals = generateRandomIntervals(maxTransTime, seqCount);
        String[] numbers = generateSeqNumber(seqCount);

        sequenceList = new ArrayList<TransactionSequence>(seqCount);
        for (int i=0; i<seqCount; i++) {
            TransactionSequence transSequence = new TransactionSequence(this, numbers[i], startTime+intervals[i]);
            sequenceList.add(transSequence);
        }

        if (isDebug) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            logger.debug(dateFormat.format(new Date(startTime)) + " - " + Arrays.toString(intervals));
        }
    }

    private String[] generateSeqNumber(int count) {
        String[] numbers = new String[count];
        for (int i=0; i<count-1; i++) {
            numbers[i] = String.valueOf(i+START_SEQ_NUM);
        }
        numbers[count-1] = String.valueOf(END_SEQ_NUM);
        return numbers;
    }

    public long[] generateRandomIntervals(long maxTotalInterval, int count) {
        long[] intervals = new long[count];
        long min = maxTotalInterval - 100 < 0 ? maxTotalInterval : 100;

        long totalInterval = Math.abs(random.nextLong()%(maxTotalInterval-min)+min);

        intervals[0] = 0;
        for (int i=1; i<count; i++) {
            intervals[i] = intervals[i-1]+totalInterval/count;
        }

        return intervals;
    }

    public boolean isRemovable() {
        boolean isRemovable = true;
        for (TransactionSequence eachSequence : sequenceList) {
            if (!eachSequence.getUsed()) {
                isRemovable = false;
                break;
            }
        }

        return isRemovable;
    }
}

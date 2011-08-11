package com.nexr.platform.search.generator.transaction;

/**
 * Created by IntelliJ IDEA.
 * User: hcnah
 * Date: 11. 8. 10.
 * Time: 오후 2:51
 * To change this template use File | Settings | File Templates.
 */
public class TransactionSequence implements Comparable<TransactionSequence> {
    private final String seqNumber;
    private final long timestamp;
    private final Transaction transaction;
    private boolean isUsed;

    public TransactionSequence(Transaction transaction, String seqNumber, long timestamp) {
        this.transaction = transaction;
        this.seqNumber = seqNumber;
        this.timestamp = timestamp;
        this.isUsed = false;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSeqNumber() {
        return seqNumber;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setUsed(boolean used) {
        this.isUsed = used;
    }

    public boolean getUsed() {
        return isUsed;
    }

    @Override
    public int compareTo(TransactionSequence o) {
        return (int) (this.getTimestamp() - o.getTimestamp());
    }
}

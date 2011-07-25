package com.nexr.platform.search.provider;

import com.nexr.platform.search.consumer.DataConsumer;

public interface DataProvider<V> {
    void start();
    void setBatchSize(int batchSize);
    void setDataConsumer(DataConsumer<V> consumer);
    long getProduceCount();
}

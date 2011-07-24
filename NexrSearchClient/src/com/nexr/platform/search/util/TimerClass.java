package com.nexr.platform.search.util;

import com.nexr.platform.search.ClientIndexer;
import com.nexr.platform.search.provider.XContentDataProvider;

import java.io.*;
import java.util.TimerTask;

public class TimerClass extends TimerTask {


    private final XContentDataProvider _provider;
    private final ClientIndexer _consumer;
    private PrintWriter _printWriter;

    private volatile double _produceAvg = 0.0d;
    private volatile double _consumeAvg = 0.0d;

    private volatile long _produceTotalCount = 0L;
    private volatile long _consumeTotalCount = 0L;

    private volatile long _minProduceTpsCount = 0L;
    private volatile long _minConsumeTpsCount = 0L;

    private volatile long _maxProduceTpsCount = 0L;
    private volatile long _maxConsumeTpsCount = 0L;

    private volatile long _currentProduceTpsCount = 0L;
    private volatile long _currentConsumeTpsCount = 0L;

    private final int _secondRate = 1000;

    private final long _startTime;

    private long _lastTime;

    public TimerClass(XContentDataProvider provider, ClientIndexer indexer, String logFilePath) throws IOException {
        _provider = provider;
        _consumer = indexer;

        _startTime = System.currentTimeMillis();
        _lastTime = _startTime;

        File file = new File(logFilePath);
        File dir = new File(logFilePath.substring(0, logFilePath.lastIndexOf("/")));

        if(!dir.isDirectory()) dir.mkdirs();

        if(file.exists()) file.delete();

        file.createNewFile();

        BufferedWriter buffWriter = new BufferedWriter(new FileWriter(file, true));
        _printWriter = new PrintWriter(buffWriter,true);
    }

    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        long currentElapsedTime = currentTime - _lastTime;
        if(currentElapsedTime <= 0)
            return;

        long totalElapsedTime = currentTime - _startTime;

        _currentProduceTpsCount = 0L;
        _currentConsumeTpsCount = 0L;

        long consumeTotalCount = 0L;

        consumeTotalCount += _consumer.getConsumeCount();


        long produceTotalCount = _provider.getProduceCount();

        _currentProduceTpsCount = ( ( produceTotalCount - _produceTotalCount ) * _secondRate / currentElapsedTime );
        _currentConsumeTpsCount = ( ( consumeTotalCount - _consumeTotalCount )  * _secondRate / currentElapsedTime );

        _produceTotalCount = produceTotalCount;
        _consumeTotalCount = consumeTotalCount;

        this.setAvgTps(totalElapsedTime);
        this.setMinMaxTps();

        this.print(totalElapsedTime);

        _lastTime = currentTime;
    }

    private void setMinMaxTps(){

        _maxProduceTpsCount = Math.max(_maxProduceTpsCount, _currentProduceTpsCount);
        _maxConsumeTpsCount = Math.max(_maxConsumeTpsCount, _currentConsumeTpsCount);

        if(_minProduceTpsCount == 0) _minProduceTpsCount = _currentProduceTpsCount;
        else _minProduceTpsCount = Math.min(_minProduceTpsCount, _currentProduceTpsCount);
        if(_minConsumeTpsCount == 0) _minConsumeTpsCount = _currentConsumeTpsCount;
        else _minConsumeTpsCount = Math.min(_minConsumeTpsCount, _currentConsumeTpsCount);
    }

    private void setAvgTps(long totalElapsedTime){
        _produceAvg = ( _produceTotalCount  * _secondRate / totalElapsedTime ) ;
        _consumeAvg = ( _consumeTotalCount * _secondRate / totalElapsedTime ) ;
    }

    private void print(long totalElapsedTime){
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(totalElapsedTime).append("\t");
        stringBuilder.append(_consumeTotalCount).append("\t");
        stringBuilder.append(_currentConsumeTpsCount).append("\t");
        stringBuilder.append(_consumeAvg).append("\t");
        stringBuilder.append(_minConsumeTpsCount).append("\t");
        stringBuilder.append(_maxConsumeTpsCount).append("\t");

        _printWriter.println(stringBuilder.toString());
        // System.out.println(stringBuilder.toString());
    }

}

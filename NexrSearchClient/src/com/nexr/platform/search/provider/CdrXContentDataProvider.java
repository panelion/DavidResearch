package com.nexr.platform.search.provider;

import com.nexr.platform.collector.record.LogRecord;
import com.nexr.platform.collector.record.LogRecordKey;
import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.router.MapRoutingEvent;
import com.nexr.platform.search.router.RoutingEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/25/11
 * Time: 10:23 AM
 */
public class CdrXContentDataProvider extends StreamDataProvider<RoutingEvent> {

    public static final String ROUTING_EVENT_DATA_TYPE = "routing.event.data.type";
    /*public static final String LOG_RECORD_TIMESTAMP_FIELD_NAME = "log.record.timestamp.field.name";
    public static final String LOG_RECORD_TIMESTAMP_FORMAT = "log.record.timestamp.format";
    public static final String DEFAULT_LOG_RECORD_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";*/


    private SequenceFile.Reader _valueReader;
    private final Properties _prof;

    /*private final String _timestampFieldName;
    private final DateFormat _timestampFormat;*/

    public CdrXContentDataProvider(String valuePath, Properties prof){

        Configuration configuration = new Configuration();
        _prof = prof;

        try {
            FileSystem fs = FileSystem.get(configuration);

            _valueReader = new SequenceFile.Reader(fs, new Path(valuePath), configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }

        _produceCount = 0;
    }

    @Override
    public DataConsumer.DataEvent<RoutingEvent> next() {

        MapRoutingEvent event = new MapRoutingEvent(_prof.getProperty(ROUTING_EVENT_DATA_TYPE, "TransactionLog"));

        try {

            LogRecordKey logRecordKey = new LogRecordKey();
            LogRecord logRecord = new LogRecord();

            if(_valueReader.next(logRecordKey, logRecord)) {

                event.setId(logRecordKey.getLogId());

                /*try {
                    event.setTimeStamp(LogRecordKey.formatter.parse(logRecordKey.getTime()).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }*/

                for(String field : logRecord.getFields()){
                    event.put(field, logRecord.getValue(field).trim());
                }

            } else {
                event = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            event = null;
        }

        if( event ==  null ) return null;

        _produceCount++;
        return new DataConsumer.DataEvent<RoutingEvent>(event);
    }

    @Override
    public void reset() {

    }
}

package com.nexr.platform.search.utils.io;

import com.nexr.platform.collector.record.LogRecord;
import com.nexr.platform.collector.record.LogRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/4/11
 * Time: 5:43 PM
 */
public class MapFileWriter {

    private String _dirPath;
    private MapFile.Writer _writer;

    public MapFileWriter(String dirPath){
        _dirPath = dirPath;
    }

    public MapFile.Writer getMapFileWriter(){
        return this._writer;
    }

    public void open() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);

        // _writer = new MapFile.Writer(conf, fs, _dirPath, LogRecordKey.class, LogRecord.class);
        _writer = new MapFile.Writer(conf, fs, _dirPath, LogRecordKey.class, LogRecord.class, SequenceFile.CompressionType.RECORD);

    }

    public void close() throws IOException {
        _writer.close();
    }
}

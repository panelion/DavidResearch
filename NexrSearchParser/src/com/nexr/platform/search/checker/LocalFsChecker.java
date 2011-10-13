package com.nexr.platform.search.checker;

import com.nexr.platform.collector.record.LogRecord;
import com.nexr.platform.collector.record.LogRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.SequenceFile;

import java.io.File;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 11.
 * Time: 오후 2:51
 */
    public class LocalFsChecker {

    private SequenceFile.Reader _reader;

    public LocalFsChecker(String localFilePath, String configFilePath) {
        /*Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(configFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Configuration configuration = new Configuration(true);

        for(Map.Entry<Object, Object> entry : prop.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
*/
        try {
            // FileSystem fs = FileSystem.get(configuration);

            Configuration configuration = new Configuration(true);
            org.apache.hadoop.fs.FileSystem fs = new RawLocalFileSystem();
            fs.initialize(new File("/").toURI(), configuration);

            _reader = new SequenceFile.Reader(fs, new Path(localFilePath), configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean read() {

        boolean rtnVal = false;
        LogRecordKey logRecordKey = new LogRecordKey();
        LogRecord logRecord = new LogRecord();
        int rowCount = 0;
        try {
            while(_reader.next(logRecordKey, logRecord)) {
                rowCount++;
                rtnVal = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            rtnVal = false;
        }

        System.out.println("ROW COUNT : " + rowCount);

        return rtnVal;
    }

    public static void main(String[] args) {
        String localFilePath, configFilePath;
        if(args.length > 0) {
            localFilePath = args[0];
            configFilePath = args[1];
        } else {
            localFilePath = "/Users/david/Execute/Data/SearchPlatform/CDR/hdfs/data";
            configFilePath = "/Users/david/Execute/nexrsearch_client/config/fsConf.conf";
        }

        LocalFsChecker localFsChecker = new LocalFsChecker(localFilePath, configFilePath);
        boolean rtnVal = localFsChecker.read();

        if(rtnVal) System.out.println("no checksum errors.");
        else System.out.println("this file has a problem.");
    }
}

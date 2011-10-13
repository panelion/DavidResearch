package com.nexr.platform.search;

import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.provider.CdrLogRecordDataProvider;
import com.nexr.platform.search.provider.DataProvider;
import com.nexr.platform.search.provider.SdpXContentDataProvider;
import com.nexr.platform.search.router.Router;
import com.nexr.platform.search.router.RouterFactory;
import com.nexr.platform.search.router.RoutingEvent;
import com.nexr.platform.search.util.TimerClass;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Timer;

/**
 * ElasticSearch 의 Indexing Client 로서의 기능을 수행 한다.
 * Produce - Consumer Pattern 으로 설계 되어져 있다.
 *
 * Mady by David.Woo - 2011.07.20
 * david.woo@nexr.com
 */
public class ClientIndexer implements DataConsumer<RoutingEvent> {

    private final Router _router;
    private volatile long _consumeCount;

    /**
     * Indexing 을 수행한 data 의 수를 가져 온다.
     * @return Indexing Count
     */
    public long getConsumeCount() {
        return _consumeCount;
    }

    /**
     * ClientIndexer 의 생성자.
     * @param properties    Node Client 관련 설정.
     */
    public ClientIndexer(Properties properties) {
        RouterFactory factory = RouterFactory.getInstance();
        _router  = factory.create(properties);
        _consumeCount = 0L;
    }

    /**
     * Data 를 Indexing 한다.
     * @param data  RoutingEvent Data - > key, Value 로 이루어 져 있다.
     */
    public void consume(Collection<DataEvent<RoutingEvent>> data) {

        if((data != null) && (data.size() > 0)) {
            for(DataEvent<RoutingEvent> route : data) {
                try{
                    _router.route(route.getData());
                    _consumeCount++;
                } catch(Exception e){
                    try {
                        _router.route(route.getData());
                        _consumeCount++;
                    }catch(Exception e2){
                        e2.printStackTrace();
                    }
                }
            }
        }

        if(data != null) data.clear();
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        String configFilePath, dataFilePath, logFilePath;
        String columnFilePath, sdComCellFilePath, sdComSecFilePath, serverIP, usedColumnFilePath;
        boolean isCdr;
        if(args.length > 0) {
            configFilePath = args[0];
            dataFilePath = args[1];
            logFilePath = args[2];
            isCdr = Boolean.parseBoolean(args[3]);

            columnFilePath = args[4];
            serverIP = args[5];
            // sdComCellFilePath = args[6];
            // sdComSecFilePath = args[7];
            usedColumnFilePath = args[6];

        } else {
            /************************************************************************************************************************
             *
             * Sdp Config or Cdr Config 둘 중에 한 가지만 설정 할 것.
             *
             ************************************************************************************************************************/

            /**
             * SDP Config in Mac Book.
             */
            /*confFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/SdpClient.conf";
            dataFilePath = "/Users/david/Data/SearchPlatform/SDP/hdfs/data";
            logFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/logs/test.log";
            isCdr = false;*/

            /**
             * SDP Config in Linux.
             */
            /*confFilePath = "/home/david/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/SdpClient.conf";
            dataFilePath = "/home/david/Data/SearchPlatform/SDP/hdfs/data";
            logFilePath = "/home/david/IdeaProjects/DavidResearch/NexrSearchClient/logs/test.log";
            isCdr = false;*/

            /* CDR Config with Sequence Local File Data */
            configFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/CdrClient.conf";
            dataFilePath = "/Users/david/Data/SearchPlatform/CDR/hdfs/data";
            logFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/logs/cdrIndexing.log";
            isCdr = true;

            /**
             * CDR Config without Sequence Local File Data
             * Properties prof
             * String columnFilePath
             * String dataFilePath
             * String serverIP
             * String sdComCellFilePath
             * String sdComSecFilePath
             */
            configFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/CdrClient.conf";
            dataFilePath = "/Users/david/Execute/Data/SearchPlatform/CDR/sample_cdr_data.csv";
            logFilePath = "/Users/david/IdeaProjects/DavidResearch/NexrSearchClient/logs/cdrIndexing.log";
            isCdr = true;

            columnFilePath = "/Users/david/Execute/nexrsearch_client/config/wcd_column.txt";
            sdComCellFilePath = "/Users/david/Execute/nexrsearch_client/config/sd_com_cell.csv";
            sdComSecFilePath = "/Users/david/Execute/nexrsearch_client/config/sd_com_sec.csv";
            serverIP = "127.0.0.1";
            usedColumnFilePath = "/Users/david/Execute/nexrsearch_client/config/wcd_used_column.txt";
        }

        /**
         * 설정 파일을 읽어 온다.
         */
        FileInputStream fs = new FileInputStream(configFilePath);
        Properties properties = new Properties();
        properties.load(fs);

        /**
         * Provider 생성.
         * BatchSize 는 Provider 에서 한번에 처리할 양.
         * 여기 서는 데이터 를 hadoop fileSystem 에서 읽어 오는 숫자로 이해 하면 된다.
         */
        DataProvider<RoutingEvent> provider = null;
        // if(isCdr) provider = new CdrXContentDataProvider(dataFilePath, properties);
         try {
            if(isCdr) provider = new CdrLogRecordDataProvider(properties, columnFilePath, dataFilePath, serverIP, usedColumnFilePath);
            else provider = new SdpXContentDataProvider(dataFilePath, properties);
            provider.setBatchSize(1000);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }


        /**
         * Consumer 생성.
         */
        DataConsumer<RoutingEvent> consumer = new ClientIndexer(properties);
        provider.setDataConsumer(consumer);


        /**
         * TPS 측정을 위한 Timer Thread Class 생성.
         */
        Timer timer = new Timer();
        TimerClass timerClass = new TimerClass(provider, consumer, logFilePath);
        timer.scheduleAtFixedRate(timerClass, 1000, 1000);

        /**
         * 시작!
         */
        provider.start();
    }
}

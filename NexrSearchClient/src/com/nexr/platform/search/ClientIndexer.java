package com.nexr.platform.search;

import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.provider.XContentDataProvider;
import com.nexr.platform.search.router.Router;
import com.nexr.platform.search.router.RouterFactory;
import com.nexr.platform.search.router.RoutingEvent;
import com.nexr.platform.search.router.RoutingException;
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

                } catch(RoutingException e){
                    e.printStackTrace();
                }
            }
        }

        if(data != null) data.clear();
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        String confFilePath, dataFilePath, logFilePath;
        if(args.length > 0) {
            confFilePath = args[0];
            dataFilePath = args[1];
            logFilePath = args[2];
        } else {
            /************************************************************************************************************************
             *
             * Sdp Config or Cdr Config 둘 중에 한 가지만 설정 할 것.
             *
             ************************************************************************************************************************/
            /* SDP Config */
            confFilePath = "/Users/panelion/IdeaProject/DavidResearch/NexrSearchClient/config/properties/SdpClient.conf";
            dataFilePath = "/Users/panelion/Data/SearchPlatform/SDP/hdfs/data";
            logFilePath = "/Users/panelion/IdeaProject/DavidResearch/NexrSearchClient/logs/test.log";

            /* CDR Config */
            /*
                confFilePath = "/home/david/Dropbox/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/CdrClient.conf";
                dataFilePath = "/home/david/Data/SearchPlatform/CDR/hdfs/data";
                logFilePath = "/home/david/Dropbox/IdeaProjects/DavidResearch/NexrSearchClient/logs/test.log";
            */
        }

        /**
         * 설정 파일을 읽어 온다.
         */
        FileInputStream fs = new FileInputStream(confFilePath);
        Properties properties = new Properties();
        properties.load(fs);

        /**
         * Provider 생성.
         * BatchSize 는 Provider 에서 한번에 처리할 양.
         * 여기 서는 데이터 를 hadoop fileSystem 에서 읽어 오는 숫자로 이해 하면 된다.
         */
        XContentDataProvider provider = new XContentDataProvider(dataFilePath, properties);
        provider.setBatchSize(1000);

        /**
         * Consumer 생성.
         */
        ClientIndexer indexer = new ClientIndexer(properties);
        provider.setDataConsumer(indexer);


        /**
         * TPS 측정을 위한 Timer Thread Class 생성.
         */
        Timer timer = new Timer();
        TimerClass timerClass = new TimerClass(provider, indexer, logFilePath);
        timer.scheduleAtFixedRate(timerClass, 1000, 1000);

        /**
         * 시작!
         */
        provider.start();
    }
}

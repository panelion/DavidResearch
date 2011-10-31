package com.nexr.platform.search;

import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.provider.DataProvider;
import com.nexr.platform.search.provider.LogGeneratorDataProvider;
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
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 10/19/11
 * Time: 10:49 AM
 */
public class ClientLogGeneratorIndexer {

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
    public ClientLogGeneratorIndexer(Properties properties) {
        RouterFactory factory = RouterFactory.getInstance();
        _router  = factory.create(properties);
        _consumeCount = 0L;
    }

    /**
     * Data 를 Indexing 한다.
     * @param data  RoutingEvent Data - > key, Value 로 이루어 져 있다.
     */
    public void consume(Collection<DataConsumer.DataEvent<RoutingEvent>> data) {

        if((data != null) && (data.size() > 0)) {
            for(DataConsumer.DataEvent<RoutingEvent> route : data) {
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


        String configFilePath = "";
        String logFilePath = "";
        int sourceCount = 0;

        if (args.length > 0) {
            configFilePath = args[0];
            logFilePath = args[1];
            sourceCount = Integer.parseInt(args[2]);
        }
        else {
            configFilePath = "/home/david/IdeaProjects/DavidResearch/NexrSearchClient/config/properties/LogGeneratorClient.conf";
            logFilePath = "/home/david/IdeaProjects/DavidResearch/NexrSearchClient/logs/testIndexing.log";
            sourceCount = 4;
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

            provider = new LogGeneratorDataProvider((String) properties.get("routing.event.data.type"), sourceCount);
            provider.setBatchSize(10000);
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

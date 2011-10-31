package com.nexr.platform.search.provider;

import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.router.MapRoutingEvent;
import com.nexr.platform.search.router.RoutingEvent;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 10/19/11
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogGeneratorDataProvider extends StreamDataProvider<RoutingEvent> {

    private final String mappingName;
    private final int sourceCount;

    public LogGeneratorDataProvider(String mappingName, int sourceCount) {
        this.mappingName = mappingName;
        this.sourceCount = sourceCount;
    }

    @Override
    public DataConsumer.DataEvent<RoutingEvent> next() {

        MapRoutingEvent event = new MapRoutingEvent(mappingName);

        event.setId(String.format("%09d", _produceCount));
        event.setTimeStamp(System.currentTimeMillis());

        for(int i = 1 ; sourceCount >= i; i++) {
            event.put("test" + i, "value" + i);
        }

        _produceCount++;
        return new DataConsumer.DataEvent<RoutingEvent>(event);
    }

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

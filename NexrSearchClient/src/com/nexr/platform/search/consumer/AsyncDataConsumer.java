package com.nexr.platform.search.consumer;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/5/11
 * Time: 5:58 PM
 */
public class AsyncDataConsumer<V> implements DataConsumer<V> {
    private volatile ConsumerThread _consumerThread;
    private volatile DataConsumer<V> _consumer;
    private LinkedList<DataEvent<V>> _batch;

    /**
     * The 'soft' size limit of each event batch. If the events are coming in too fast and
     * it already accumulate this many, then we block the incoming events until the number of
     * buffered events drop below this limit after some of them being sent to background
     * DataConsumer.
     */
    private int _batchSize;

    public AsyncDataConsumer()
    {
        _batch = new LinkedList<DataEvent<V>>();
        _batchSize = 1; // default
        _consumerThread = null;
    }

    /**
     * Start the background thread that batch-processes the incoming data events by sending them to the background DataConsumer.
     * <br>
     * If this method is not called, all threads trying to send in data events will eventually be blocked.
     */
    public void start()
    {
        _consumerThread = new ConsumerThread();
        _consumerThread.setDaemon(true);
        _consumerThread.start();
    }

    /**
     * Set the background DataConsumer.
     * @param consumer the DataConsumer that actually consumes the data events.
     */
    public void setDataConsumer(DataConsumer<V> consumer)
    {
        synchronized(this)
        {
            _consumer = consumer;
        }
    }

    public DataConsumer<V> getDataConsumer(){
        return _consumer;
    }

    /**
     * consumption of a collection of data events. Note that this method may have a side
     * effect. That is it may empty the Collection passed in after execution. <br><br>
     * Duplicates and buffers the incoming data events.<br><br>
     * If too many (>=_batchSize) amount of data events are already buffered,
     * it waits until the background DataConsumer consumes some of the events before
     * it add new events to the buffer. This throttles the amount of events in each batch.
     *
     * @param data
     * @throws IOException
     *
     */
    public void consume(Collection<DataEvent<V>> data) throws IOException
    {
        if (data == null || data.size() == 0) return;

        synchronized(this)
        {
            while(_batch.size() >= _batchSize)
            {
                if(_consumerThread == null || !_consumerThread.isAlive() || _consumerThread._stop)
                {
                    throw new IOException("consumer thread has stopped");
                }
                try { this.wait(); }
                catch (InterruptedException ignored){}
            }
            for(DataEvent<V> event : data)
            {
                _batch.add(event);
            }
            this.notifyAll(); // wake up the thread waiting in flushBuffer()
        }
    }

    protected final void flushBuffer()
    {
        LinkedList<DataEvent<V>> currentBatch;

        synchronized(this)
        {
            while(_batch.size() == 0)
            {
                if(_consumerThread._stop) return;
                try { this.wait();}
                catch (InterruptedException ignored) { }
            }
            currentBatch = _batch;
            _batch = new LinkedList<DataEvent<V>>();

            this.notifyAll(); // wake up the thread waiting in consume(...)
        }

        if(_consumer != null)
        {
            try
            {
                _consumer.consume(currentBatch);
            }
            catch (Exception e)
            {
                System.err.println(e.getMessage());
            }
        }
    }

    private final class ConsumerThread extends Thread
    {
        boolean _stop = false;

        ConsumerThread()
        {
            super("ConsumerThread");
        }

        public void run()
        {
            while(!_stop)
            {
                flushBuffer();
            }
        }
    }
}

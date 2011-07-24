package com.nexr.platform.search.provider;

import com.nexr.platform.search.ClientIndexer;
import com.nexr.platform.search.consumer.DataConsumer;
import com.nexr.platform.search.consumer.DataConsumer.DataEvent;
import com.nexr.platform.search.router.Router;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public abstract class StreamDataProvider<V> implements DataProvider<V> {

    private int _batchSize;
	private DataConsumer _consumer;
	private DataThread<V> _thread;

	public StreamDataProvider()
	{
		_batchSize=1;
		_consumer=null;
	}

	public void setDataConsumer(DataConsumer consumer)
	{
	  _consumer = consumer;
	}

	public abstract DataEvent<V> next();

	public abstract void reset();

	public void setBatchSize(int batchSize) {
		_batchSize=Math.max(1, batchSize);
	}

	public void start() {
		if (_thread==null || !_thread.isAlive())
		{
			reset();
			_thread = new DataThread<V>(this);
			_thread.start();
		}
	}

	private static final class DataThread<V> extends Thread
	{
	    private Collection<DataConsumer.DataEvent<V>> _batch;
		private final StreamDataProvider<V> _dataProvider;

        DataThread(StreamDataProvider<V> dataProvider)
		{
			super("Stream DataThread");
			setDaemon(false);
			_dataProvider = dataProvider;
			_batch = new LinkedList<DataConsumer.DataEvent<V>>();
		}

		@Override
		public void start()
		{
		  super.start();
		}

		private void flush()
	    {
		    Collection<DataEvent<V>> tmp = _batch;
            _batch = new LinkedList<DataConsumer.DataEvent<V>>();

		    try
	        {
		      if(_dataProvider._consumer != null)
              {
		    	  _dataProvider._consumer.consume(tmp);
		      }
	        }
	        catch (IOException e)
	        {
	          System.err.println(e.getMessage());
	        }
	    }

		public void run()
		{
            while (!Thread.currentThread().isInterrupted())
            {
                DataConsumer.DataEvent<V> data = _dataProvider.next();

                if (data != null)
                {
                    _batch.add(data);
                    if (_batch.size()>=_dataProvider._batchSize) {
                      flush();
                    }
                } else {
                    flush();

                    this.interrupt();
                    System.exit(0);
                }
            }
        }
	}

}

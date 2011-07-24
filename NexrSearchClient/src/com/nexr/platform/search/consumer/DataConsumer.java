package com.nexr.platform.search.consumer;

import java.io.IOException;
import java.util.Collection;

public interface DataConsumer<V> {

	public static final class DataEvent<V>
	{
		private V _data;

		public DataEvent(V data)
		{
			_data=data;
		}

		public V getData()
		{
			return _data;
		}
	}

	void consume(Collection<DataEvent<V>> data) throws IOException;
}

package net.intelie.challenges.concrete;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import net.intelie.challenges.*;

public class ConcurrentHashMapEventStore implements EventStore {
	
	private ConcurrentHashMap<String,List<Event>> event_store;
	
	
	public ConcurrentHashMapEventStore() {
		event_store = new ConcurrentHashMap<String,List<Event>>();
	}
	

	public void insert(Event event) {
		List<Event> stream = event_store.get(event.type());

		if(stream == null) 
		{
			addNewStream(event);
			insert(event);
			return;
		}
		
		stream.add(event);
			
	}

	@Override
	public void removeAll(String type) {
		List<Event> stream = event_store.get(type);
		if(stream != null) stream.clear();
		return;
	}
 
	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		List<Event> stream = event_store.get(type);
		
		if(stream == null) stream = new ArrayList<Event>();
		
		return new ConcurrentHashMapIterator(stream, startTime, endTime);
	}

	
	/**
	 * This method uses put if absent to handle concurrency issues
	 */
	private void addNewStream(Event event) {
		List<Event> stream = new ArrayList<Event>();
		event_store.putIfAbsent(event.type(),stream);
	}
	
	

	
}

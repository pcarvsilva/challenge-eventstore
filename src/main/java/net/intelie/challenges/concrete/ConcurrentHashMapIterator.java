package net.intelie.challenges.concrete;

import java.util.List;
import net.intelie.challenges.*;

public class ConcurrentHashMapIterator implements EventIterator {

	
	private List<Event> stream;
	private int current_index;
	private long startTime;
	private long endTime;
	private Event current = null;
	
	public ConcurrentHashMapIterator(List<Event> stream,long startTime ,long endTime) {
		this.stream = stream;
		this.endTime = endTime;
		this.startTime = startTime;
		current_index = -1;
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public boolean moveNext() {
		try {
			while(true){
				current_index++;
				
				Event event = stream.get(current_index);
				if(event == null) continue;
				if(event.timestamp() < startTime) continue;
				if(event.timestamp() >= endTime)  continue;
				
				current = event;
				break;
			}
			return true;
		}
		catch(IndexOutOfBoundsException e)
		{
			current = null;
			return false;
		}
	}

	
	@Override
	public Event current() throws IllegalStateException {
		if(current == null) throw new IllegalStateException();
		return current;
	}

	@Override
	public void remove() {
		stream.remove(current());
	}
	
	
}

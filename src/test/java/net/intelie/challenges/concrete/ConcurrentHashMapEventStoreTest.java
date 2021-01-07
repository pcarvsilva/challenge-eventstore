package net.intelie.challenges.concrete;

import static org.junit.Assert.*;
import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

public class ConcurrentHashMapEventStoreTest {

	@Test
    public void mustInsertWithoutError() throws Exception {
		
		EventStore store = new ConcurrentHashMapEventStore();
        Event event = new Event("someType", 123L);
		store.insert(event);
		
		try(EventIterator stream = store.query("someType", 123L, 125L))
		{
			while (stream.moveNext()) 
			{
				assertEquals("Event should be on stream", event.type(),"someType");
			}
		}
    }
	
	@Test
    public void mustQueryOnlyForSameEventType() throws Exception {
		
		EventStore store = new ConcurrentHashMapEventStore();
		
		String first_type = "someType";
        String second_type = "someOtherType";
		
		Event event = new Event(first_type, 123L);
		store.insert(event);
		
		Event event2 = new Event(second_type, 123L);
		store.insert(event2);
		
		try(EventIterator stream = store.query(first_type, 123L, 125L))
		{
			while (stream.moveNext()) 
			{
				assertEquals("Events on stream should all be of type " + first_type,first_type, event.type());
			}
		}
    }
	
	@Test
	
    public void mustClearStreamAfterRemoveAll() throws Exception {
		
		EventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		Event event = new Event(type, 123L);
		store.insert(event);
		store.removeAll(type);
		
		try(EventIterator stream = store.query(type, 123L, 125L))
		{
			assertFalse("Stream Should Be Empty", stream.moveNext());
		}
    }
	 
    
    @Test
    public void mustFilterStreamByTimeStamp() throws Exception {
		
		EventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		long lowerBound = 123L;
		long upperBound = 130L;
		
		Event eventBellowLowerBound = new Event(type, lowerBound -1);
		store.insert(eventBellowLowerBound);
		
		Event eventOnLowerBound = new Event(type, lowerBound);
		store.insert(eventOnLowerBound);
		
		Event eventOnMiddle = new Event(type, (lowerBound + upperBound)/2);
		store.insert(eventOnMiddle);
		
		Event eventOnUpperBound = new Event(type, upperBound);
		store.insert(eventOnUpperBound);

		Event eventOverUpperBound = new Event(type, upperBound + 1);
		store.insert(eventOverUpperBound);
		
		try(EventIterator stream = store.query(type, 123L, 125L)){
			while (stream.moveNext()) 
			{
				assertEquals("Event time stamp sould be bigger or equals to "  + lowerBound, true,stream.current().timestamp() >= lowerBound);
				assertEquals("Event time stamp sould be smaller than " + upperBound, true,stream.current().timestamp() <  upperBound);
			}
		}
    }

    @Test
    public void mustRemoveEventDuringQuery() throws Exception {
		
		ConcurrentHashMapEventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		Event event = new Event(type, 123L);
		store.insert(event);
		
		try(EventIterator stream = store.query(type, 123L, 125L))
		{
			while (stream.moveNext()) 
			{
				stream.remove();
			}
		}
		
		try(EventIterator stream = store.query(type, 123L, 125L))
		{
			assertFalse("Stream Should Be Empty", stream.moveNext());
		}
		
    }
    
    @Test
    public void mustRemoveEvenWithNoStream() throws Exception {
		
		ConcurrentHashMapEventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		store.removeAll(type);
		
    }
    
    @Test(expected = IllegalStateException.class)
    public void mustThrowExeptionIfMoveNextIsNotCalled() throws Exception
    {
		ConcurrentHashMapEventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		Event event = new Event(type, 123L);
		store.insert(event);
		
		try(EventIterator stream = store.query(type, 123L, 125L))
		{
			stream.current();
		}
    }
    
    @Test(expected = IllegalStateException.class)
    public void mustThrowExeptionOnFinishedQuery() throws Exception
    {
		ConcurrentHashMapEventStore store = new ConcurrentHashMapEventStore();
		
		String type = "someType";
		
		Event event = new Event(type, 123L);
		store.insert(event);
		
		try(EventIterator stream = store.query(type, 123L, 125L))
		{
			while (stream.moveNext()){}
			stream.current();
		}
    }
    

	
}

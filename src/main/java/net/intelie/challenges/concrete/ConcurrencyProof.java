package net.intelie.challenges.concrete;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import net.intelie.challenges.*;

public class ConcurrencyProof {

	
	private static EventStore store;
	private final static int defaultNumberOfThreads = 5;
	private final static long timeStampLimit = 5L;
	
	private enum Commands
	{
		INSERT,
		REMOVE,
		QUERY,
		REMOVEALL
	}
	
	
	public static void main(String args[])  //static method  
	{  
		store = new ConcurrentHashMapEventStore();
		
		ThreadPoolExecutor poll = (ThreadPoolExecutor) Executors.newFixedThreadPool(defaultNumberOfThreads);
		
		boolean run = true;
		while(run)
		{
			poll.execute(() -> {
					try {
						generateRandomCommand();
					} 
					catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			});
		}
		
	}
	
	private static void insert()
	{
		System.out.println("Inserting Data");
		String type = generateRandomType();
		long timeStamp = (long) (timeStampLimit * new Random().nextFloat());
		Event event = new Event(type,timeStamp);
		store.insert(event);
	}
	
	private static void query() throws Exception
	{
		System.out.println("Querying Data");
		String type = generateRandomType();
		long upperBound = (long) (timeStampLimit * new Random().nextFloat());			
		try(EventIterator stream = store.query(type, 0, upperBound)){
			while (stream.moveNext()) { }
		}
	}

	private static String generateRandomType() {
		String type = Integer.toString(new Random().nextInt(4));
		return type;
	}
	
	private static void remove() throws Exception
	{
		System.out.println("Querying and Removing Data");
		String type = generateRandomType();
		long upperBound = (long) (timeStampLimit * new Random().nextFloat());	
		try(EventIterator stream = store.query(type, 0, upperBound)){
			if(stream.moveNext()) 
				stream.remove();
			while (stream.moveNext()) { }
		}
	}
	
	private static void removeAll() throws Exception
	{
		System.out.println("Removing a Stream");
		String type = generateRandomType();
		long upperBound = (long) (timeStampLimit * new Random().nextFloat());	
		try(EventIterator stream = store.query(type, 0, upperBound)){
			while (stream.moveNext()) { }
		}
	}
	
	
	private static void generateRandomCommand() throws Exception
	{

		int choosenEnum = new Random().nextInt(4);
		switch(Commands.values()[choosenEnum])
		{
			case INSERT:
				insert();
				break;
			case QUERY:
				query();
				break;
			case REMOVE:
				remove();
				break;
			case REMOVEALL:
				removeAll();
				break;
			default:
				break;
		}
			
	}
}


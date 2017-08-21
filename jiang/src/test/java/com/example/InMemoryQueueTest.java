package com.example;


import org.junit.Test;

import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;


public class InMemoryQueueTest extends BaseQueueTest
{

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		queueService = new InMemoryQueueService();
	}

	public void testCreateQueue() throws ExecutionException, InterruptedException
	{
		InMemoryQueueService queueService = new InMemoryQueueService();
		UUID qName = queueService.createQueue().get();
		assertTrue( ( queueService ).queueMap.containsKey( qName ) );
	}

	public void testDisposeQueue() throws ExecutionException, InterruptedException
	{
		InMemoryQueueService memoryQueueService = new InMemoryQueueService();
		UUID qName = memoryQueueService.createQueue().get();
		assertTrue( memoryQueueService.queueMap.containsKey( qName ) );
		assertTrue( memoryQueueService.disposeQueue( qName ).get() );
		assertFalse( memoryQueueService.queueMap.containsKey( qName ) );
	}


	public void testPushMessageWithoutDelay() throws InterruptedException, ExecutionException
	{
		InMemoryQueueService memoryQueueService = new InMemoryQueueService();
		UUID qName = memoryQueueService.createQueue().get();
		assertTrue( memoryQueueService.queueMap.containsKey( qName ) );

		UUID msgId = memoryQueueService.push( qName, 0, test_message ).get();
		Future<Message> future = getMessageFromUnderlyingQueue( memoryQueueService.queueMap.get( qName ), 50L );
		Message msg = future.get();
		assertNotNull( msg );
		assertTrue( msg.getId().equals( msgId ) );
		assertTrue( msg.getContent().equals( test_message ) );
	}

	public void testPushMessageWithDelay() throws InterruptedException, ExecutionException
	{
		Random random = new Random();
		final long delay = random.nextInt( 200 ) + delayToCheck;
		InMemoryQueueService memoryQueueService = new InMemoryQueueService();
		UUID qName = memoryQueueService.createQueue().get();

		assertTrue( memoryQueueService.queueMap.containsKey( qName ) );

		System.out.print( "Testing invisible time(ms): " + delay );
		Future<UUID> msgId = memoryQueueService.push( qName, delay, test_message );

		assertNull( memoryQueueService.queueMap.get( qName ).peek() ); //with delay it should be null;

		singleThreadedexecutor.schedule( new Runnable()
		{
			@Override
			public void run()
			{
				Message msg = memoryQueueService.queueMap.get( qName ).peek();
				assertNull( msg ); // before timeout of the delay, it should be null;
			}
		}, delay - 10, TimeUnit.SECONDS );

		Future<Message> future = getMessageFromUnderlyingQueue( memoryQueueService.queueMap.get( qName ), delay );
		Message msg = future.get();
		assertNotNull( msg );
		assertEquals( msg.getId(), msgId.get() );
	}

	@Test( expected = java.util.concurrent.TimeoutException.class )
	public void testQueueCapacity() throws InterruptedException, ExecutionException, TimeoutException
	{
		InMemoryQueueService memoryQueueService = new InMemoryQueueService( 1 );
		UUID qName = memoryQueueService.createQueue().get();
		assertTrue( memoryQueueService.queueMap.containsKey( qName ) );

		UUID msgId = memoryQueueService.push( qName, 0, test_message ).get();
		try
		{
			memoryQueueService.push( qName, 0, test_message2 ).get( 50, TimeUnit.MILLISECONDS );
		}
		catch( Exception e )
		{
			assertTrue( e instanceof TimeoutException );
		}
	}


	public void testPollMessageWithoutDelete() throws InterruptedException, ExecutionException
	{
		int delay = 0;
		InMemoryQueueService memoryQueueService = new InMemoryQueueService();
		UUID qName = memoryQueueService.createQueue().get();
		final Future<UUID> futureMsgId = memoryQueueService.push( qName, delay, test_message );
		final UUID msgUuid = futureMsgId.get();
		CompletableFuture<UUID> futureReceipt = new CompletableFuture();
		Future<String> content = memoryQueueService.pull( qName, new Consumer<Future<UUID>>()
		{
			@Override
			public void accept( Future<UUID> uuidFuture )
			{
				try
				{
					futureReceipt.complete( uuidFuture.get() );
				}
				catch( Exception e )
				{
					e.printStackTrace();
					futureReceipt.complete( null );
				}
			}
		} );

		assertEquals( test_message, content.get() );
		assertEquals( msgUuid, futureReceipt.get() );

		assertTrue( memoryQueueService.invisibleMsg.containsKey( msgUuid ) ); //the message should be kept in the invisible map
		assertFalse( memoryQueueService.invisibleMsg.get( msgUuid ).isVisible() ); //the message should be set to invisible
		assertFalse( memoryQueueService.queueMap.get( qName ).contains( msgUuid ) ); //the message should be removed from queue

		//to test the message be put back into the queue after the invisible time
		singleThreadedexecutor.schedule( new Runnable()
		{
			@Override
			public void run()
			{
				assertFalse( memoryQueueService.invisibleMsg.containsKey( msgUuid ) ); //the message should be removed from the invisible map
				Message message = memoryQueueService.queueMap.get( qName ).peek();
				assertEquals( msgUuid, message.getId() ); //the message should be put back int queue
				assertTrue( message.isVisible() ); //the message shoubld be set to visible
			}
		}, InMemoryQueueService.INVISIBLE_TIME_MILLISECOND + delayToCheck, TimeUnit.MILLISECONDS );
	}


	public void testPollMessageAndDelete() throws InterruptedException, ExecutionException
	{
		int delay = 0;
		InMemoryQueueService memoryQueueService = new InMemoryQueueService();
		UUID qName = memoryQueueService.createQueue().get();
		final Future<UUID> futureMsgId = memoryQueueService.push( qName, delay, test_message );
		final UUID msgUuid = futureMsgId.get();
		CompletableFuture<UUID> promiseUuid = new CompletableFuture();
		Future<String> content = memoryQueueService.pull( qName, new Consumer<Future<UUID>>()
		{
			@Override
			public void accept( Future<UUID> uuidFuture )
			{
				try
				{
					promiseUuid.complete( uuidFuture.get() );
				}
				catch( Exception e )
				{
					e.printStackTrace();
					promiseUuid.complete( null );
				}
			}
		} );

		assertEquals( test_message, content.get() );

		UUID receipt = promiseUuid.get();
		assertEquals( msgUuid, receipt );
		if( memoryQueueService.delete( receipt ).get() )
		{
			assertFalse( memoryQueueService.invisibleMsg.containsKey( receipt ) );
		}
		assertFalse( memoryQueueService.queueMap.get( qName ).contains( receipt ) );

	}


	private Future<Message> getMessageFromUnderlyingQueue( Queue<Message> queue, long timeInMilliSecond )
	{
		return singleThreadedexecutor.schedule( new Callable<Message>()
		{
			@Override
			public Message call()
			{
				Message msg = queue.peek();
				while( msg == null )
				{
					try
					{
						Thread.sleep( 50L );
					}
					catch( InterruptedException e )
					{
						e.printStackTrace();
					}
					msg = queue.peek();
				}
				return msg;
			}
		}, timeInMilliSecond, TimeUnit.MILLISECONDS );
	}


	public void testPollMessageInEmptyQueue() throws InterruptedException, ExecutionException
	{
		int delay = 0;
		UUID qName = queueService.createQueue().get();
		CompletableFuture<UUID> futureReciept = new CompletableFuture();
		Future<String> content = queueService.pull( qName, new Consumer<Future<UUID>>()
		{
			@Override
			public void accept( Future<UUID> uuidFuture )
			{
				try
				{
					futureReciept.complete( uuidFuture.get() );
				}
				catch( Exception e )
				{
					e.printStackTrace();
					futureReciept.complete( null );
				}
			}
		} );

		assertNull( content.get() );
		assertNull( futureReciept.get() );
	}

	public void testMultipleThreads() throws ExecutionException, InterruptedException
	{
		UUID qName = queueService.createQueue().get();
		int msgCount = 20;
		System.out.println( "the number of message to fill the circular queue is " + msgCount );
		final Future<UUID>[] futures = new Future[ msgCount ];
		for( int i = 0; i < msgCount; i++ )//write point should be reach the end of file.
		{
			final int index = i;
			multipleThreadedexecutor.execute( new Runnable()
			{
				@Override
				public void run()
				{
					futures[ index ] = queueService.push( qName, index, test_message );
				}
			} );
		}

		for( int i = 0; i < msgCount; i++ )//pull all the messages
		{
			final int index = i;
			queueService.pull( qName, new Consumer<Future<UUID>>()
			{
				@Override
				public void accept( Future<UUID> uuidFuture )
				{
					try
					{
						assertEquals( futures[ index ].get(), uuidFuture.get() );
					}
					catch( InterruptedException e )
					{
						e.printStackTrace();
					}
					catch( ExecutionException e )
					{
						e.printStackTrace();
					}
				}
			} );
		}


	}
}

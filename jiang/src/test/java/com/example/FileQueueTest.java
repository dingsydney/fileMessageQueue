package com.example;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;


/**
 * todo. All tests, that have message push and pull, should have at least two instance of FileQueueService. One for pushing message, the other for pull. However there is a bug in FileQueueSerivce.
 * The bug is that the invisibleQueue should not be kept in memory. it should be kept in file so that it shared by different process.
 */

public class FileQueueTest extends BaseQueueTest
{
	//
	// Implement me if you have time.
	//

	String filePath;

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		queueService = new FileQueueService();
		filePath = System.getProperty( "user.home" ) + File.separator;
	}

	/**
	 * whenever a queue created, a file named with queue uuid should be created.
	 */
	public void testCreateQueue() throws ExecutionException, InterruptedException, IOException
	{
		Future<UUID> future = queueService.createQueue();
		UUID qName = future.get();
		File file = new File( filePath + qName );
		assertTrue( file.exists() );
		RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
		assertEquals( randomAccessFile.readLong(), FileQueueService.MESSAGE_POINTER_POSITION );
		assertEquals( randomAccessFile.readLong(), FileQueueService.MESSAGE_POINTER_POSITION );
		try
		{
			randomAccessFile.readUTF();
		}
		catch( Exception e )
		{
			assertTrue( e instanceof EOFException );
		}
		randomAccessFile.close();
		assertTrue( queueService.disposeQueue( qName ).get() );

	}

	/**
	 * whenever a queue disposed, the underlying file should be removed.
	 */
	public void testDisposeQueue() throws ExecutionException, InterruptedException, IOException
	{
		Future<UUID> future = queueService.createQueue();
		UUID qName = future.get();
		Future<Boolean> futureBoolean = queueService.disposeQueue( qName );
		futureBoolean.get();
		File file = new File( filePath + qName );
		assertFalse( file.exists() );

	}

	/**
	 * After a message is put in tot the queue. the content of the underlying file should be:
	 * readpos: point to the first message
	 * writepos: file length
	 * the uuid of the message and content of the message.
	 */
	public void testPushMessageWithoutDelay() throws ExecutionException, InterruptedException, IOException, ClassNotFoundException
	{
		Future<UUID> future = queueService.createQueue();
		UUID qName = future.get();

		Future<UUID> futureMsgId = queueService.push( qName, 0, test_message );
		UUID msgId = futureMsgId.get();

		File file = new File( filePath + qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
		long readpoint = randomAccessFile.readLong();
		long writePoint = randomAccessFile.readLong();
		assertEquals( readpoint, FileQueueService.MESSAGE_POINTER_POSITION );
		assertEquals( writePoint, randomAccessFile.length() );
		String uuidStr = randomAccessFile.readUTF();
		String content = randomAccessFile.readUTF();
		assertEquals( UUID.fromString( uuidStr ), msgId );
		assertEquals( content, test_message );
		randomAccessFile.close();
		assertTrue( queueService.disposeQueue( qName ).get() );
	}

	/**
	 * put multiple message, check the file content is right
	 */
	public void testMultiplePushMessageWithoutDelay() throws ExecutionException, InterruptedException, IOException, ClassNotFoundException
	{
		Future<UUID> future = queueService.createQueue();
		UUID qName = future.get();

		int msgNum = 10;
		String[] messages = new String[ msgNum ];
		Future<UUID>[] futures = new Future[ msgNum ];
		for( int i = 0; i < msgNum; i++ )
		{
			messages[ i ] = i == 0 ? test_message : messages[ i - 1 ] + test_message;
			futures[ i ] = queueService.push( qName, 0, messages[ i ] );
		}

		for( int i = 0; i < msgNum; i++ )
		{
			futures[ i ].get();
		}

		File file = new File( filePath + qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
		long readpoint = randomAccessFile.readLong();
		long writePoint = randomAccessFile.readLong();
		assertEquals( readpoint, FileQueueService.MESSAGE_POINTER_POSITION );
		assertEquals( writePoint, randomAccessFile.length() );
		randomAccessFile.seek( FileQueueService.MESSAGE_POINTER_POSITION );
		for( int i = 0; i < msgNum; i++ )
		{
			String uuidStr = randomAccessFile.readUTF();
			String content = randomAccessFile.readUTF();
			assertEquals( UUID.fromString( uuidStr ), futures[ i ].get() );
			assertEquals( content, messages[ i ] );
		}
		randomAccessFile.close();
		assertTrue( queueService.disposeQueue( qName ).get() );

	}


	/**
	 * before the delayed time, the underly file should be unchanged.
	 * afterwards check as {@link #testMultiplePushMessageWithoutDelay()}
	 */
	public void testPushMessageWithDelay() throws ExecutionException, InterruptedException, IOException, ClassNotFoundException
	{
		Random random = new Random();
		final long delay = random.nextInt( 200 ) + delayToCheck;
		System.out.print( "Testing invisible time(ms): " + delay );
		Future<UUID> future = queueService.createQueue();
		UUID qName = future.get();

		Future<UUID> futureMsgId = queueService.push( qName, delay, test_message );

		Future<Exception> futureCheckFile = singleThreadedexecutor.schedule( new Callable<Exception>()
		{
			@Override
			public Exception call() throws Exception
			{
				File file = new File( filePath + qName );
				RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
				long readLong = randomAccessFile.readLong();
				long writeLong = randomAccessFile.readLong();
				try
				{
					String uuidStr = randomAccessFile.readUTF();
					String content = randomAccessFile.readUTF();
					return null;
				}
				catch( EOFException e )
				{
					return e;
				}
				catch( IOException e )
				{
					throw e;
				}
			}
		}, 0, TimeUnit.MILLISECONDS );

		assertTrue( futureCheckFile.get() instanceof EOFException );

		UUID msgId = futureMsgId.get();

		File file = new File( filePath + qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
		long readLong = randomAccessFile.readLong();
		long writeLong = randomAccessFile.readLong();
		String uuidStr = randomAccessFile.readUTF();
		String content = randomAccessFile.readUTF();
		assertEquals( UUID.fromString( uuidStr ), msgId );
		assertEquals( content, test_message );
		assertTrue( queueService.disposeQueue( qName ).get() );
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
		assertTrue( queueService.disposeQueue( qName ).get() );
	}

	public void testPollMessageAndDelete() throws InterruptedException, ExecutionException
	{
		int delay = 0;
		UUID qName = queueService.createQueue().get();
		final Future<UUID> futureMsgId = queueService.push( qName, delay, test_message );
		final UUID msgUuid = futureMsgId.get();
		CompletableFuture<UUID> promiseUuid = new CompletableFuture();

		Future<String> content = queueService.pull( qName, new Consumer<Future<UUID>>()
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
		if( queueService.delete( receipt ).get() )
		{
			assertFalse( queueService.invisibleMsg.containsKey( receipt ) );
		}
		assertTrue( queueService.disposeQueue( qName ).get() );

	}

	public void testPollMessageWithoutDelete() throws Exception
	{
		int delay = 0;
		UUID qName = queueService.createQueue().get();
		final Future<UUID> futureMsgId = queueService.push( qName, delay, test_message );
		final UUID msgUuid = futureMsgId.get();
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

		assertEquals( test_message, content.get() );
		assertEquals( msgUuid, futureReciept.get() );

		assertTrue( queueService.invisibleMsg.containsKey( msgUuid ) ); //the message should be kept in the invisible map
		assertFalse( queueService.invisibleMsg.get( msgUuid ).isVisible() ); //the message should be set to invisible
		queueService.getLock( qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( filePath + qName, "rw" );
		assertEquals( randomAccessFile.readLong(), randomAccessFile.readLong() ); //read points should be same as write point in the file;
		randomAccessFile.close();
		queueService.releaseLock( qName );

		//to test the message be put back into the queue after the invisible time
		singleThreadedexecutor.schedule( new Callable<Void>()
		{
			@Override
			public Void call() throws Exception
			{
				assertFalse( queueService.invisibleMsg.containsKey( msgUuid ) ); //the message should be removed from the invisible map
				Message msg = readMessage( qName );
				assertEquals( msg.getContent(), test_message );
				assertEquals( msg.getId(), msgUuid );
				return null;
			}
		}, InMemoryQueueService.INVISIBLE_TIME_MILLISECOND + delayToCheck, TimeUnit.MILLISECONDS );
		assertTrue( queueService.disposeQueue( qName ).get() );
	}

	/**
	 * this read message does not move the read pointer in the file
	 */
	private Message readMessage( UUID qName ) throws Exception
	{
		queueService.getLock( qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( filePath + qName, "rw" );
		randomAccessFile.seek( FileQueueService.READ_POINTER_POSITION );
		long postion = randomAccessFile.readLong();
		randomAccessFile.seek( postion );
		String uuidStr = randomAccessFile.readUTF();
		String content = randomAccessFile.readUTF();
		randomAccessFile.close();
		queueService.releaseLock( qName );

		return new Message( UUID.fromString( uuidStr ), content );
	}


	/**
	 * test the circular query.
	 * Scenario will be:
	 * 1. write message till full.  The next write will not be completed. By this point, the read pointer is in the beginning of file, while the write point is at the end of file
	 * 2. read all the message, except the last one of step 1,  checking UUID are matches. The write point goes to the beginning of file, and the read point is at the end of file.
	 * 3. read the last message of step 1. check the message UUID. now both of the pointer are at the beginning of file.
	 * 4. read message should return null;
	 */
	public void testCircularQuery() throws ExecutionException, InterruptedException
	{
		int capacity = 1000;
		queueService = new FileQueueService( capacity );
		UUID qName = queueService.createQueue().get();
		int msgLen = qName.toString().length() + test_message.length() + 4;
		int msgCount = capacity / msgLen;
		System.out.println( "the number of message to fill the circular queue is " + msgCount );
		final Future<UUID>[] futures = new Future[ msgCount ];
		for( int i = 0; i < msgCount; i++ )//write point should be reach the end of file.
		{
			futures[ i ] = queueService.push( qName, i, test_message );
		}

		//write more shoulb be bl0cked.
		Future<UUID> notCompletedFutureMsq = queueService.push( qName, 0, test_message2 );
		try
		{
			notCompletedFutureMsq.get( 200L, TimeUnit.MILLISECONDS );
		}
		catch( Exception e )
		{
			assertTrue( e instanceof TimeoutException );
		}

		//end of step 1.

		Future<String>[] futureStrings = new Future[ msgCount ];
		for( int i = 0; i < msgCount; i++ )//pull all the first batch messages
		{
			final int index = i;
			futureStrings[ index ] = queueService.pull( qName, new Consumer<Future<UUID>>()
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

		for( Future<String> futrue : futureStrings )
		{
			futrue.get();
		}
		//end of step 2

		final UUID msqInFileHeader = notCompletedFutureMsq.get();
		//read the last message availl
		queueService.pull( qName, new Consumer<Future<UUID>>()
		{
			@Override
			public void accept( Future<UUID> uuidFuture )
			{
				try
				{
					assertEquals( msqInFileHeader, uuidFuture.get() );
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
		} ).get();
		//end of step 3;

		assertNull( queueService.pull( qName, null ).get() );
		assertTrue( queueService.disposeQueue( qName ).get() );

	}

}

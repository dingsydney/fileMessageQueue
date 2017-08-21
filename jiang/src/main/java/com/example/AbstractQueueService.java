package com.example;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by jiadin on 9/06/2016.
 * Provide template for concrete queue service.
 */
public abstract class AbstractQueueService implements QueueService
{

	static final Message NULL_MESSAGE = new Message( UUID.fromString( "1-1-1-1-1" ), null );
	static long INVISIBLE_TIME_MILLISECOND = 10000;
	protected final ScheduledExecutorService pushExecutor = Executors.newScheduledThreadPool( 1 );
	protected final ScheduledExecutorService pullExecutor = Executors.newScheduledThreadPool( 3 );

	/**
	 * to trace messages which is retrieved and scheduled to put back to the queue.
	 * These message can be explicitly be deleted.
	 * To delete, the message need to be removed from the map and set to be deleted.
	 * The schedule should check the deletion flag when try to put back the message to queue
	 */
	final Map<UUID,Message> invisibleMsg = new ConcurrentHashMap<>( 1023 );


	public AbstractQueueService()
	{
		pullExecutor.scheduleAtFixedRate( new Runnable()
		{
			@Override
			public void run()
			{
				cleanInvisibleQueue();
			}
		}, 50000, INVISIBLE_TIME_MILLISECOND * 10, TimeUnit.MICROSECONDS );
	}

	private void cleanInvisibleQueue()
	{
		Set<UUID> keys = invisibleMsg.keySet();
		for( UUID key : keys )
		{
			Message m = invisibleMsg.get( key );
			if( ( System.currentTimeMillis() - m.pulledTIme ) > INVISIBLE_TIME_MILLISECOND )
			{
				invisibleMsg.remove( key );
			}
		}
	}

	private void putMessageIntoInvisibleList( Message message )
	{
		invisibleMsg.put( message.getId(), message );
		message.pulledTIme = System.currentTimeMillis();
	}

	/**
	 * put the message into queue
	 * block the pusher, if the queue is full.
	 */
	abstract protected void putMessageInQueue( final UUID qName, final Message msg ) throws Exception;

	/**
	 * used to put back the invisible message
	 */
	protected void schedulePutMessageInQueue( final UUID qName, final Message msg, long delayInMilliSecond ) throws Exception
	{
		pushExecutor.schedule( new Callable<Void>()
		{
			@Override
			public Void call() throws Exception
			{
				putMessageInQueue( qName, msg );
				return null;
			}

			;
		}, delayInMilliSecond, TimeUnit.MILLISECONDS );
	}

	abstract Message pullMessageFromQueue( UUID qName ) throws Exception;


	@Override
	public Future<String> pull( UUID qName, Consumer<Future<UUID>> receipt )
	{
		final Future<Message> futureMsg = pullExecutor.schedule( new Callable<Message>()
		{
			@Override
			public Message call() throws Exception
			{
				getLock( qName );
				Message msg = pullMessageFromQueue( qName );
				releaseLock( qName );
				if( msg == null ) return AbstractQueueService.NULL_MESSAGE;
				msg.setVisible( false );
				putMessageIntoInvisibleList( msg );
				schedulePutMessageInQueue( qName, msg, INVISIBLE_TIME_MILLISECOND );
				return msg;

			}
		}, 0, TimeUnit.MILLISECONDS );

		CompletableFuture<String> promise = new CompletableFuture<>();
		pullExecutor.schedule( new Callable<Void>()
		{
			@Override
			public Void call() throws Exception
			{
				CompletableFuture<UUID> promiseReceipt = new CompletableFuture<>();
				Message msg = futureMsg.get();
				if( msg != AbstractQueueService.NULL_MESSAGE )
				{
					promise.complete( msg.getContent() );
					promiseReceipt.complete( msg.getId() );
				}
				else
				{
					promise.complete( null );
					promiseReceipt.complete( null );
				}
				if( receipt != null )
				{
					receipt.accept( promiseReceipt );
				}
				return null;
			}
		}, 0, TimeUnit.MILLISECONDS );

		return promise;
	}

	public void dispose()
	{
		pushExecutor.shutdownNow();
	}

	protected void getLock( UUID qName ) throws Exception
	{
		return;
	}

	protected void releaseLock( UUID qName )
	{
		return;
	}

	@Override
	public Future<Boolean> delete( final UUID msgId )
	{
		return pullExecutor.schedule( new Callable<Boolean>()
		{
			@Override
			public Boolean call() throws Exception
			{
				Message message = invisibleMsg.remove( msgId );
				if( message != null )
				{
					message.setDeleted( true );  //in multiple threading,  there is chance the message is set to delete and still be put back into queue.
					return true;
				}
				else
				{
					return false;
				}
			}
		}, 0, TimeUnit.MILLISECONDS );
	}
}

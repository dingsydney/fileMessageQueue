package com.example;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService extends AbstractQueueService
{
	//
	// Task 2: Implement me.
	//

	/**
	 * message holder keyed by queue id which is UUID
	 */
	final Map<UUID,Queue<Message>> queueMap;
	private int capacity;

	public InMemoryQueueService()
	{
		this( 100000 );
	}

	public InMemoryQueueService( int capacity )
	{
		this.capacity = capacity;
		queueMap = new ConcurrentHashMap<>( 1023 );
	}

	/**
	 * todo: need to check the limitation of max queue allowed
	 */
	public Future<UUID> createQueue()
	{
		return pushExecutor.schedule( new Callable<UUID>()
		{
			@Override
			public UUID call() throws Exception
			{
				UUID uuid = UUID.randomUUID();
				Queue<Message> queue = new LinkedBlockingDeque<Message>( capacity );
				queueMap.put( uuid, queue );
				return uuid;
			}
		}, 0, TimeUnit.MILLISECONDS );

	}

	public Future<Boolean> disposeQueue( final UUID qName )
	{
		return pullExecutor.schedule( new Callable<Boolean>()
		{
			@Override
			public Boolean call() throws Exception
			{
				Queue<Message> q = queueMap.remove( qName );
				if( q == null ) return true;
				q.clear();
				return true;
			}
		}, 0, TimeUnit.MILLISECONDS );
	}

	public Future<UUID> push( UUID qName, long delayInMilliSecond, String msg )
	{
		return pushExecutor.schedule( new Callable<UUID>()
		{
			@Override
			public UUID call() throws Exception
			{
				Queue<Message> queue = getQueue( qName );
				if( queue == null )
				{
					throw new Exception( "queue " + qName + "does not exist." );
				}
				else
				{
					UUID msgId = UUID.randomUUID();
					Message msgObj = new Message( msgId, msg );
					putMessageInQueue( qName, msgObj );
					return msgId;
				}
			}
		}, delayInMilliSecond, TimeUnit.MILLISECONDS );
	}

	private Queue<Message> getQueue( UUID qName ) throws Exception
	{
		Queue<Message> q = queueMap.get( qName );
		if( q == null ) throw new Exception( qName + " does not exist." );
		return q;
	}

	@Override
	public void dispose()
	{
		super.dispose();
		queueMap.clear();
	}

	protected Message pullMessageFromQueue( UUID qName ) throws Exception
	{
		Queue<Message> queue = getQueue( qName );
		if( queue == null )
		{
			throw new Exception( qName + " does not exist." );
		}
		else
		{
			return queue.poll();
		}
	}

	protected void putMessageInQueue( final UUID qName, final Message msg ) throws Exception
	{
		Queue<Message> queue = getQueue( qName );
		while( !queue.offer( msg ) )
		{
			Thread.sleep( 20L );
			if( msg.isDeleted() ) break;
			queue = getQueue( qName );
			if( queue == null )
			{
				throw new Exception( qName + " does not exist." );
			}
		}
		if( !msg.isVisible() )
		{
			invisibleMsg.remove( msg.getId() );
		}
	}


}

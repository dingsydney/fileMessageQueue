package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class SqsQueueService implements QueueService
{
	//
	// Task 4: Optionally implement parts of me.
	//
	// This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
	// primarily so you can quickly assess your choices for method signatures in QueueService in
	// terms of how well they map to the implementation intended for a production environment.
	//

	public SqsQueueService( AmazonSQSClient sqsClient )
	{
	}

	@Override
	public Future<UUID> createQueue()
	{
		return null;
	}

	@Override
	public Future<Boolean> disposeQueue( UUID qName )
	{
		return null;
	}

	@Override
	public Future<UUID> push( UUID qName, long delayInMilliSecond, String msg )
	{
		return null;
	}

	@Override
	public Future<String> pull( UUID qName, Consumer<Future<UUID>> receipt )
	{
		return null;
	}

	@Override
	public Future<Boolean> delete( UUID msgId )
	{
		return null;
	}
}

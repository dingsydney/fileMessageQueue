package com.example;

import junit.framework.TestCase;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public abstract class BaseQueueTest extends TestCase
{
	ScheduledExecutorService singleThreadedexecutor;
	ScheduledExecutorService multipleThreadedexecutor;
	String test_message = "TEST MESSAGE";
	String test_message2 = "TEST MESSAGE2";
	int delayToCheck = 10;


	protected AbstractQueueService queueService;

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		singleThreadedexecutor = Executors.newScheduledThreadPool( 1 );
		multipleThreadedexecutor = Executors.newScheduledThreadPool( 4 );
	}
}

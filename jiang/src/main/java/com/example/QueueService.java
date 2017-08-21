package com.example;


import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface QueueService
{

	//
	// Task 1: Define me.
	//
	// This interface should include the following methods.  You should choose appropriate
	// signatures for these methods that prioritise simplicity of implementation for the range of
	// intended implementations (in-memory, file, and SQS).  You may include additional methods if
	// you choose.
	//
	// - push
	//   pushes a message onto a queue.
	// - pull
	//   retrieves a single message from a queue.
	// - delete
	//   deletes a message from the queue that was received by pull().
	//

	Future<UUID> createQueue();

	Future<Boolean> disposeQueue( UUID qName );

	Future<UUID> push( UUID qName, long delayInMilliSecond, String msg );

	Future<String> pull( UUID qName, Consumer<Future<UUID>> receipt );

	Future<Boolean> delete( UUID msgId );
}

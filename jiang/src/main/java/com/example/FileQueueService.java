package com.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * A circular queue backed by a file.
 * File has three sections:
 * the first long contains the read pointer to pull message from file
 * the second long contains the write pointer to push message into tile.
 * then followed by messages.
 * messages contains two string: uuis string, and message content string.
 *
 * The FileQueueService does not work well in windows releated to file operations.
 */
public class FileQueueService extends AbstractQueueService
{

	static final long FILE_MAX_LENGTH = 2 * 1024 * 1024;
	static final String FILE_SUFFIX_LOCK = ".lock";

	static final long READ_POINTER_POSITION = 0L;
	static final long WRITE_POINTER_POSITION = 8L;
	static final long MESSAGE_POINTER_POSITION = 16L;

	final String path;

	private final long sizeLimit;


	public FileQueueService()
	{
		this( FILE_MAX_LENGTH, System.getProperty( "user.home" ) + File.separator );
	}

	public FileQueueService( long sizeLimit )
	{
		this( sizeLimit, System.getProperty( "user.home" ) + File.separator );
	}

	public FileQueueService( long fileMaxLength, String path )
	{
		this.sizeLimit = fileMaxLength;
		this.path = path;

	}

	@Override
	public Future<UUID> createQueue()
	{
		return pushExecutor.schedule( new Callable<UUID>()
		{
			@Override
			public UUID call() throws Exception
			{
				UUID uuid = UUID.randomUUID();
				File file = getMessageFile( uuid );
				if( file.createNewFile() )
				{
					RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
					try
					{
						randomAccessFile.writeLong( MESSAGE_POINTER_POSITION );
						randomAccessFile.writeLong( MESSAGE_POINTER_POSITION );
					}
					finally
					{
						randomAccessFile.close();
					}
					return uuid;
				}
				else
				{
					throw new Exception( "create queue failed due to can't not create a new file" );
				}
			}
		}, 0, TimeUnit.MILLISECONDS );
	}

	@Override
	public Future<Boolean> disposeQueue( UUID qName )
	{
		return pushExecutor.schedule( new Callable<Boolean>()
		{
			@Override
			public Boolean call() throws Exception
			{
				File file = getMessageFile( qName );
				getLock( qName );
				if( !file.delete() )
				{
					System.err.println( "delete file fails." + file.getName() );
				}
				releaseLock( qName );
				return true;
			}
		}, 0, TimeUnit.MILLISECONDS );
	}

	@Override
	public Future<UUID> push( UUID qName, long delayInMilliSecond, String msg )
	{
		return pushExecutor.schedule( new Callable<UUID>()
		{
			@Override
			public UUID call() throws Exception
			{
				final UUID msqId = UUID.randomUUID();
				final Message msgObj = new Message( msqId, msg );
				putMessageInQueue( qName, msgObj );
				return msqId;
			}

		}, delayInMilliSecond, TimeUnit.MILLISECONDS );
	}

	/**
	 * write a message into file.
	 *
	 * @return true. the message is wriiten into file. false, the message is not writtng into file due to the file is full.
	 */
	private boolean writeObject( RandomAccessFile randomAccessFile, Message msgObj ) throws IOException
	{
		long readingPos = randomAccessFile.readLong();
		long writingPos = randomAccessFile.readLong();
		String uuidStr = msgObj.getId().toString();
		String content = msgObj.getContent();
		int sizeofMessage = uuidStr.length() + content.length() + 2 + 2;  //2 extra bytes for every utf string.

		if( randomAccessFile.length() >= sizeLimit )
		{
			if( readingPos > writingPos )
			{ //writing point is behind of the reading point.
				//after writing, the wriing pos should not to beyond the reading pos
				if( writingPos + sizeofMessage >= readingPos ) //circular queue full. can't write any message
				{
					return false;
				}
			}
			else
			{ // the write point is in front of reading point.
				//check whether need to write in the begining of file
				if( writingPos + sizeofMessage > randomAccessFile.length() )
				{ // the writing pointer go back to the start of file
					writingPos = MESSAGE_POINTER_POSITION;
					if( writingPos + sizeofMessage > readingPos )
					{ //again. the writing pointer can't go beyond the reading pos;
						return false;
					}
				}//else there is enough space for new message

			}
		}
		randomAccessFile.seek( writingPos );
		randomAccessFile.writeUTF( uuidStr );
		randomAccessFile.writeUTF( content );
		randomAccessFile.seek( WRITE_POINTER_POSITION );
		randomAccessFile.writeLong( writingPos + sizeofMessage );

		return true;
	}


	private File getMessageFile( UUID qName ) throws Exception
	{
		return new File( path + qName );
	}


	protected void getLock( UUID qName ) throws Exception
	{
		File file = new File( path + qName + FILE_SUFFIX_LOCK );
		boolean rtn = false;
		while( !( rtn = file.createNewFile() ) )
		{
			try
			{
				Thread.sleep( 50 );
			}
			catch( InterruptedException e )
			{
				break;
			}
		}
	}

	protected void releaseLock( UUID qName )
	{
		File file = new File( path + qName + FILE_SUFFIX_LOCK );
		if( !file.delete() )
		{
			System.err.println( "can't delete file " + qName + ". The related queue is be blocked." );
		}
	}

	/**
	 * @return Message read from file, null if no message readable in file.
	 */
	protected Message pullMessageFromQueue( UUID qName ) throws Exception
	{
		File file = getMessageFile( qName );
		RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
		String uuidStr = null;
		String content = null;
		try
		{
			long readingPos = randomAccessFile.readLong();
			long writingPos = randomAccessFile.readLong();
			if( readingPos > writingPos )  //if read pointer is in front of write point, need to test whether it is reach the end of file.
			{
				if( readingPos >= randomAccessFile.length() )
				{
					readingPos = MESSAGE_POINTER_POSITION; //reach end of file, go to first message postiion in file.
				}
			}
			if( readingPos == writingPos ) return null; //no message lest........
			uuidStr = randomAccessFile.readUTF();
			content = randomAccessFile.readUTF();
			long filePos = randomAccessFile.getFilePointer();
			randomAccessFile.seek( READ_POINTER_POSITION );
			randomAccessFile.writeLong( filePos ); //move the reading pointer to the next postion.
		}
		catch( IOException e )
		{
			randomAccessFile.close();
		}

		return new Message( UUID.fromString( uuidStr ), content );
	}

	protected void putMessageInQueue( final UUID qName, final Message msgObj ) throws Exception
	{
		File file = getMessageFile( qName );
		boolean writeSuccess = false;
		do
		{
			try
			{
				if( msgObj.isDeleted() ) return;
				getLock( qName );
				if( msgObj.isDeleted() ) return;
				if( !file.exists() ) throw new Exception( qName + " does not exist." );
				RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
				try
				{
					if( writeSuccess = writeObject( randomAccessFile, msgObj ) )
					{
						if( !msgObj.isVisible() )
						{
							invisibleMsg.remove( msgObj.getId() );
						}
						return;
					}
				}
				finally
				{
					randomAccessFile.close();
				}

			}
			catch( IOException e )
			{
				System.err.println( "Error in putting message into file " + file.getName() );
				e.printStackTrace();
				throw e;
			}
			finally
			{
				releaseLock( qName );
			}
		} while( !writeSuccess );
	}


}

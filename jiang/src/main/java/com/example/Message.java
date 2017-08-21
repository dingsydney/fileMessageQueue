package com.example;

import java.io.Serializable;
import java.util.UUID;

/**
 *
 */
public class Message implements Serializable
{
	private UUID id;
	private final String content;
	private transient boolean isVisible = true;
	private transient boolean isDeleted;
	transient long pulledTIme;

	public Message( UUID id, String content )
	{
		this.id = id;
		this.content = content;
	}

	public UUID getId()
	{
		return id;
	}

	public boolean isVisible()
	{
		return isVisible;
	}


	public void setVisible( boolean visible )
	{
		isVisible = visible;
	}

	public String getContent()
	{
		return content;
	}

	public boolean isDeleted()
	{
		return isDeleted;
	}

	public void setDeleted( boolean deleted )
	{
		isDeleted = deleted;
	}
}

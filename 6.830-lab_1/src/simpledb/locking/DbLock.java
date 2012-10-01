package simpledb.locking;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import simpledb.Permissions;
import simpledb.TransactionId;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.page.PageId;

/**
 * non re-entrant read-write lock
 * 
 * @author felix
 *
 */
public class DbLock 
{
	private ReentrantReadWriteLock lock;
	private PageId pid;
	
	public DbLock(PageId pid) {
		this.pid = pid;
		lock = new ReentrantReadWriteLock();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void lock(TransactionId tid, Permissions permission) throws TransactionAbortedException
	{
		if (permission == Permissions.READ_ONLY)
		{
			if (lock.getReadHoldCount() == 0)
			{
				try
				{
					while(!lock.readLock().tryLock())
					{
						wait();
					}
				}
				catch(InterruptedException e)
				{
					throw new TransactionAbortedException(tid + " aborted");
				}
			}
		}
		else
		{
			if (lock.getWriteHoldCount() == 0)
			{
				if (!lock.writeLock().tryLock())
				{
					if (!tryUpgradeLock(tid))
					{
						try
						{
							while(!lock.writeLock().tryLock())
							{
								wait();
							}
						}
						catch(InterruptedException e)
						{
							throw new TransactionAbortedException(tid + " aborted");
						}
					}
				}
			}
		}
	}
	
	public void unlock(TransactionId tid)
	{
		if(isExclusiveLock())
		{
			lock.writeLock().unlock();
			if (lock.getReadHoldCount() > 0)
			{
				lock.readLock().unlock();
			}
		}
		else
		{
			lock.readLock().unlock();
		}
		notifyAll();
	}
	
	public boolean hasLock(TransactionId tid)
	{
		return false;
	}
	
	private boolean isExclusiveLock()
	{
		return lock.isWriteLocked();
	}
	
	private synchronized boolean tryUpgradeLock(TransactionId tid)
	{
		if (!isExclusiveLock())
		{
			if(lock.getReadHoldCount() > 0)
			{
				lock.readLock().unlock();
				return lock.writeLock().tryLock();
			}
		}
		return false;
	}
}

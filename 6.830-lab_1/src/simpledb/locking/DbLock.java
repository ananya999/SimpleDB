package simpledb.locking;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import simpledb.Permissions;
import simpledb.TransactionId;
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
	private Set<TransactionId> transactions;
	private PageId pid;
	
	public DbLock(PageId pid) {
		this.pid = pid;
		lock = new ReentrantReadWriteLock();
		transactions = new HashSet<TransactionId>();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void lock(TransactionId tid, Permissions permission)
	{
		if (permission == Permissions.READ_ONLY)
		{
			if (lock.getReadHoldCount() == 0)
			{
				try
				{
					lock.readLock().lockInterruptibly(); // call to lockInterruptibly() instead
					//System.out.println("2 " + Thread.currentThread().getName());
					LockManager.getInstance().addLockedPage(tid, pid);
					transactions.add(tid);
				}
				catch(InterruptedException e)
				{
					
				}
			}
		}
		else
		{
			if (lock.getWriteHoldCount() == 0)
			{
				if (lock.writeLock().tryLock())
				{
					//System.out.println("1 " + Thread.currentThread().getName());
					//System.out.println(lock);
					LockManager.getInstance().addLockedPage(tid, pid);
					transactions.add(tid);
				}
				else
				{
					if (!tryUpgradeLock(tid))
					{
						try
						{
							lock.writeLock().lockInterruptibly();
						}
						catch(InterruptedException e)
						{
							
						}
					}
				}
			}
		}
	}
	
	public void unlock(TransactionId tid)
	{
		transactions.remove(tid);
		LockManager.getInstance().removeLockedPage(tid, pid);
		
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
	}
	
	public boolean hasLock(TransactionId tid)
	{
		return transactions.contains(tid);
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

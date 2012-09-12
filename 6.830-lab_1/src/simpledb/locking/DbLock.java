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
	public void lock(TransactionId tid, Permissions permission,String action)
	{
		if (permission == Permissions.READ_ONLY)
		{
			if (lock.getReadHoldCount() == 0)
			{
				try
				{
					lock.readLock().lockInterruptibly(); // call to lockInterruptibly() instead
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
					LockManager.getInstance().addLockedPage(tid, pid);
					transactions.add(tid);
				}
				else
				{
					if (!tryUpgradeLock(tid,action))
					{
						try
						{
							lock.writeLock().lockInterruptibly();
							System.out.println("lock " + Thread.currentThread().getName() + " "  + action+ " "  + tid + " " + pid);
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
		transactions.remove(tid);
		LockManager.getInstance().removeLockedPage(tid, pid);
	}
	
	public boolean hasLock(TransactionId tid)
	{
		return transactions.contains(tid);
	}
	
	private boolean isExclusiveLock()
	{
		return lock.isWriteLocked();
	}
	
	private synchronized boolean tryUpgradeLock(TransactionId tid, String action)
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

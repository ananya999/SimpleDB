package simpledb.locking;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import simpledb.Permissions;
import simpledb.TransactionId;
import simpledb.page.PageId;


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
			System.out.println(isExclusiveLock());
			lock.readLock().lock();
			LockManager.getInstance().addLockedPage(tid, pid);
			transactions.add(tid);
		}
		else
		{
			if (lock.writeLock().tryLock()) 
			{
				LockManager.getInstance().addLockedPage(tid, pid);
				transactions.add(tid);
			}
			else
			{
				if (!tryUpgradeLock(tid))
				{
					lock.writeLock().lock();
				}
			}
		}
	}
	
	public void unlock(TransactionId tid)
	{
		if(isExclusiveLock())
		{
			lock.writeLock().unlock();
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
	
	private synchronized boolean tryUpgradeLock(TransactionId tid)
	{
		if (!isExclusiveLock() && isUpgradeLockPermited(tid))
		{
			lock.readLock().unlock();
			lock.writeLock().lock();
			return true;
		}
		return false;
	}
	
	private  boolean isUpgradeLockPermited(TransactionId tid) 
	{
		return transactions.size() == 1 && transactions.contains(tid);
	}
	
}

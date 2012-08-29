package simpledb;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import simpledb.page.PageId;


public class DbLock 
{
	ReentrantReadWriteLock lock;
	List<Map.Entry<TransactionId, Permissions>> lockQueue;
	Set<TransactionId> transactions;
	
	public DbLock() {
		lock = new ReentrantReadWriteLock();
		lockQueue = new ArrayList<Map.Entry<TransactionId, Permissions>>();
		transactions = new HashSet<>();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void lock(TransactionId tid, PageId pid, Permissions permission)
	{
		if (permission == Permissions.READ_ONLY)
		{
			if (lock.readLock().tryLock()) 
			{
				LockManager.getInstance().grantLock(tid, pid);
				transactions.add(tid);
			}
			else
			{
				lockQueue.add(new AbstractMap.SimpleEntry(tid, permission));
			}
		}
		else
		{
			if (lock.writeLock().tryLock()) 
			{
				LockManager.getInstance().grantLock(tid, pid);
				transactions.add(tid);
			}
			else
			{
				if (!tryUpgradeLock(tid))
				{
					lockQueue.add(new AbstractMap.SimpleEntry(tid, permission));
				}
			}
		}
	}
	
	public void unlock(TransactionId tid, PageId pid)
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
		LockManager.getInstance().releaseLock(tid, pid);
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
	
	public  boolean isUpgradeLockPermited(TransactionId tid) 
	{
		return transactions.size() == 1 && transactions.contains(tid);
	}
	
}

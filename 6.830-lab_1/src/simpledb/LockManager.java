package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import simpledb.page.PageId;

public class LockManager {

	// transaction table.
	private Map<TransactionId, HashSet<PageId>> transactionMap = new HashMap <TransactionId, HashSet<PageId>>();
	// lock table.
	private Map<PageId, DbLock> lockMap= new HashMap<PageId, DbLock>();
	
	public static LockManager getInstance()
	{
		return null;
		
	}
	
	
	private LockManager() {

	}
	
    /**
	 *return DbLock	
	 *put lock if not already exist in lock table.
     *  
     */
	public DbLock getLock(PageId pid) 
	{
		DbLock lock = lockMap.get(pid);
		if(lock == null)
		{
			lock = new DbLock();
			lockMap.put(pid, lock);
		}
		return lock;
	}

	
	public void releaseAllLocks(TransactionId tid) 
	{
		transactionMap.remove(tid);
		
	}
	/**
	 * called by DbLock on unlock
	 * 
	 * @param tid
	 * @param pid
	 */
	void releaseLock(TransactionId tid, PageId pid)
	{
		HashSet<PageId> pages = transactionMap.get(tid);
		pages.remove(pid);
	}
	
	/**
	 * called by DbLock on lock
	 * 
	 * @param tid
	 * @param pid
	 */
	void grantLock(TransactionId tid, PageId pid)
	{
		HashSet<PageId> pages = transactionMap.get(tid);
		if (pages == null)
		{
			pages = new HashSet<>();
			pages.add(pid);
			transactionMap.put(tid, pages);
		}
		else
		{
			pages.add(pid);
		}
		
	}
	
	void evictPage(PageId pid)
	{
		lockMap.remove(pid);
	}
	
}

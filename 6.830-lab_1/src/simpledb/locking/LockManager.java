package simpledb.locking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import simpledb.TransactionId;
import simpledb.page.PageId;

public class LockManager {

	// transaction table.
	private Map<TransactionId, HashSet<PageId>> transactionMap = new HashMap <TransactionId, HashSet<PageId>>();
	// lock table.
	private Map<PageId, DbLock> lockMap= new HashMap<PageId, DbLock>();
	private static LockManager lockManager = null;
	
	private LockManager() 
	{
	}
	public Set<PageId> getLockedPages(TransactionId tid)
	{
		
		HashSet<PageId> pages = transactionMap.get(tid);
		if (pages == null)
		{
			pages = new HashSet<PageId>();
		}
		return new HashSet<PageId>(pages);
	}
	
	public static LockManager getInstance()
	{
		if (lockManager == null)
		{
			lockManager = new LockManager();
		}
		return lockManager;
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
			lock = new DbLock(pid);
			lockMap.put(pid, lock);
		}
		return lock;
	}
	
	/**
	 * called by DbLock on unlock
	 * 
	 * @param tid
	 * @param pid
	 */
	protected void removeLockedPage(TransactionId tid, PageId pid)
	{
		HashSet<PageId> pages = transactionMap.get(tid);
		pages.remove(pid);
		if (pages.isEmpty())
		{
			transactionMap.remove(tid);
		}
	}
	
	/**
	 * called by DbLock on lock
	 * 
	 * @param tid
	 * @param pid
	 */
	protected void addLockedPage(TransactionId tid, PageId pid)
	{
		HashSet<PageId> pages = transactionMap.get(tid);
		if (pages == null)
		{
			pages = new HashSet<PageId>();
			pages.add(pid);
			transactionMap.put(tid, pages);
		}
		else
		{
			pages.add(pid);
		}
		
	}
	
	public void evictPage(PageId pid)
	{
		lockMap.remove(pid);
	}
	
}

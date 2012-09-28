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
	private Map<PageId, HashSet<TransactionId>> pageIdmap = new HashMap<PageId, HashSet<TransactionId>>();
	private DeadlockDependencyList dependencyList;
	private DeadlockDetective detective;
	private static LockManager lockManager = new LockManager();
	
	private LockManager() 
	{
		dependencyList = new DeadlockDependencyList();
		detective = new DeadlockDetective(dependencyList);
		detective.init();
	}
	
	public void reset() 
	{
		transactionMap.clear();
		lockMap.clear();
		pageIdmap.clear();
		dependencyList.reset();
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
		return lockManager;
	}
	
    /**
	 * return DbLock	
	 * put lock if not already exist in lock table.
     *  
     */
	public synchronized DbLock getLock(PageId pid) 
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
	 * 
	 * 
	 * @param tid
	 * @param pid
	 */
	public synchronized void removeLockedPage(TransactionId tid, PageId pid)
	{
		HashSet<PageId> pages = transactionMap.get(tid);
		pages.remove(pid);
		if (pages.isEmpty())
		{
			transactionMap.remove(tid);
			dependencyList.removeTransaction(tid);
		}
		
		HashSet<TransactionId> transactions = pageIdmap.get(pid);
		transactions.remove(tid);
		if (transactions.isEmpty())
		{
			pageIdmap.remove(pid);
		}
	}
	
	/**
	 * 
	 * 
	 * @param tid
	 * @param pid
	 */
	public synchronized void addLockedPage(TransactionId tid, PageId pid)
	{
		removeFromDependecyList(tid, pid);
		addTransactionMap(tid, pid);
		addPageIdMap(tid, pid);
		
	}
	
	public synchronized void addLockRequest(TransactionId tid, PageId pid) 
	{
		addToDependecyList(tid, pid);
	}
	
	public TransactionId getDeadlockVictim(Set<TransactionId> conflictingTransactions) 
	{
		int min = Integer.MAX_VALUE;
		TransactionId result = null;
		for (TransactionId id : conflictingTransactions) 
		{
			HashSet<PageId> pages = transactionMap.get(id);
			if (pages.size() < min)
			{
				min = pages.size();
				result = id;
			}
		}
		return result;
	}
	
	private void removeFromDependecyList(TransactionId tid, PageId pid) 
	{
		Set<TransactionId> grantedTransactions = pageIdmap.get(pid);
		if (grantedTransactions != null && !grantedTransactions.isEmpty())
		{
			for (TransactionId granted : grantedTransactions) 
			{
				if (!tid.equals(granted))
				{
					dependencyList.removeEdge(tid, granted);
				}
			}
		}
	}
	
	private void addToDependecyList(TransactionId tid, PageId pid) {
		Set<TransactionId> grantedTransactions = pageIdmap.get(pid);
		if (grantedTransactions != null && !grantedTransactions.isEmpty()) 
		{
			for (TransactionId granted : grantedTransactions) 
			{
				if (!tid.equals(granted))
				{
					dependencyList.addEdge(tid, granted);
				}
			}
		}
	}
	
	private void addPageIdMap(TransactionId tid, PageId pid) 
	{
		HashSet<TransactionId> transacations = pageIdmap.get(pid);
		if (transacations == null)
		{
			transacations = new HashSet<TransactionId>();
			transacations.add(tid);
			pageIdmap.put(pid, transacations);
		}
		else
		{
			transacations.add(tid);
		}
	}
	
	private void addTransactionMap(TransactionId tid, PageId pid) {
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
}

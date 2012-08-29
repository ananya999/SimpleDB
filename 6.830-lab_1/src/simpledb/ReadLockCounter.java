package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import simpledb.page.PageId;

public class ReadLockCounter 
{

	private Map<PageId, Set<TransactionId>> m_pageTransactions = new HashMap<PageId, Set<TransactionId>>();
	

	public  void addTransaction(PageId pid, TransactionId tid) 
	{
		Set<TransactionId> transactionGroup = m_pageTransactions.get(pid);
		if (transactionGroup == null)
		{
			transactionGroup = new HashSet<TransactionId>();
			transactionGroup.add(tid);
			m_pageTransactions.put(pid, transactionGroup);
		}
		transactionGroup.add(tid);
	}

	public void removeTransaction(PageId pid, TransactionId tid) 
	{
		Set<TransactionId> transacations = m_pageTransactions.get(pid);
		transacations.remove(pid);
		
	}
	
	public  boolean isUpgradeLockPermited(PageId pid, TransactionId tid) 
	{
		Set<TransactionId> transactionGroup = m_pageTransactions.get(pid);
		return transactionGroup != null && transactionGroup.size() == 1 && transactionGroup.contains(tid);
	}


	@Override
	public String toString() {
		return "ReadLockCounter [m_pageTransactions=" + m_pageTransactions
				+ "]";
	}

}

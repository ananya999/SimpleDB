package simpledb.locking;

import simpledb.TransactionId;

/**
 * @author felix
 * 
 * implement "waits-for graph" to detect deadlock cycles. 
 * The nodes correspond to active transactions, 
 * and there is an arc from Ti to 
 * Tj if (and only if) Ti is waiting for Tj to release a lock.
 *
 */

public class DeadlockDetective 
{
	
	
	
	public void addEdge(TransactionId tid1 , TransactionId tid2)
	{
		
	}
	
	public void removeEdge(TransactionId tid1 , TransactionId tid2)
	{
		
	}
}

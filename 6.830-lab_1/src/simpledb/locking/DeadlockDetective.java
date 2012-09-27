package simpledb.locking;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import simpledb.TransactionId;

public class DeadlockDetective 
{
    Timer timer;
    DeadlockDependencyList dependencyList;

    public DeadlockDetective(DeadlockDependencyList list) {
        timer = new Timer();
        dependencyList = list;
    }
    
    public void init()
    {
    	timer.schedule(new DeadlockDetectiveTask(),0 , 5*1000);
    }

    class DeadlockDetectiveTask extends TimerTask 
    {

		@Override
		public void run() 
		{
			LockManager lockManager = LockManager.getInstance();
			Set<TransactionId> conflictingTransactions = dependencyList.getConflictingTransactions();
			TransactionId victim = lockManager.getDeadlockVictim(conflictingTransactions);
			if (victim != null)
			{
				victim.getThread().interrupt();
			}
		}
    }
}

package simpledb;

import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId {
    static AtomicLong counter = new AtomicLong(0);
    long myid;
    Thread t;
    

	public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }
    

    public boolean equals(Object tid) {
        if(tid == null)
        {
        	return false;
        }
    	return ((TransactionId)tid).myid == myid;
    }

    public int hashCode() {
        return (int) myid;
    }
    
    @Override
	public String toString() {
		return "TransactionId [myid=" + myid + "]";
	}

	public Thread getThread() 
	{
		return t;
	}

	public void setThread(Thread currentThread) 
	{
		t = currentThread;
	}
}

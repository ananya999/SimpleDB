package simpledb.operators;
import java.io.IOException;
import java.util.NoSuchElementException;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.Debug;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.tuple.IntField;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
	
	private int tableid;
	private DbIterator child;
	private TransactionId t;
	private BufferPool bufferPool;
	private boolean called = false;
	
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid) throws DbException 
    {
        this.tableid = tableid;
        this.child = child;
        this.t = t;
    	bufferPool = Database.getBufferPool();
    }

    public TupleDesc getTupleDesc() 
    {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException 
    {
        child.open();
    }

    public void close() 
    {
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException 
    {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException 
    {
        int rowCount = 0;
        Tuple tuple = null;
    	try 
        {
    		
			if (!called) 
			{
				called = true;
				for (; child.hasNext();) 
				{
					bufferPool.insertTuple(t, tableid, child.next());
					rowCount++;
				}
				tuple = buildRowCountTuple(rowCount);
			}
		} 
        catch (NoSuchElementException e) 
		{
			Debug.log("%s", e);
		} 
        catch (IOException e) 
		{
        	Debug.log("%s", e);
		}
    	return tuple;
    }

	private Tuple buildRowCountTuple(int rowCount) 
	{
		Type[] typeAr = new Type[]{Type.INT_TYPE};
		TupleDesc td = new TupleDesc(typeAr);
		Tuple t = new Tuple(td);
		t.setField(0, new IntField(rowCount));
		return t;
	}
}

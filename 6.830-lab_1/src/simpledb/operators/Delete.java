package simpledb.operators;

import java.util.NoSuchElementException;

import simpledb.BufferPool;
import simpledb.Catalog;
import simpledb.Database;
import simpledb.Debug;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.DbFile;
import simpledb.file.HeapFile;
import simpledb.tuple.IntField;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

	private int tableid;
	private DbIterator child;
	private TransactionId t;
	private BufferPool bufferPool;
	private boolean called;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
	public Delete(TransactionId t, int tableid, DbIterator child) 
    {
    	this.child = child;
    	this.tableid = tableid;
    	this.t = t;
    	bufferPool = Database.getBufferPool();
    }

    public TupleDesc getTupleDesc() {
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
					bufferPool.deleteTuple(t, tableid, child.next());
					rowCount++;
				}
				HeapFile dbFile = (HeapFile)Database.getCatalog().getDbFile(tableid);
				dbFile.savePagesToDisk();
				tuple = buildRowCountTuple(rowCount);
			}
		} 
        catch (NoSuchElementException e) 
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

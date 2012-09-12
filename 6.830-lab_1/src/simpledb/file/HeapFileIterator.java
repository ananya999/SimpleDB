package simpledb.file;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.Permissions;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.page.HeapPage;
import simpledb.page.HeapPageId;
import simpledb.tuple.Tuple;

public class HeapFileIterator implements DbFileIterator {

	
	private BufferPool pool = Database.getBufferPool();
	private HeapPageId currentPageId;
	private Iterator<Tuple> currentTupleIter;
	private HeapFile heapFile;
	private boolean isOpen = false; 
	private TransactionId tid;
	
	public HeapFileIterator(HeapFile heapFile, TransactionId tid)
	{
		this.heapFile = heapFile;
		this.tid = tid;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException 
	{
		if (heapFile.numPages() > 0)
		{
			isOpen = true;
			currentPageId = new HeapPageId(heapFile.getId(), 0);
			HeapPage page = (HeapPage) pool.getPage(tid, currentPageId, Permissions.READ_ONLY,null);
			currentTupleIter = page.iterator();
		}
		else
		{
			throw new DbException("file id " + heapFile.getId() + " not contains any page");
		}
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException 
	{
		if (isOpen == false)
		{
			return false;
		}
		return currentTupleIter.hasNext() ;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException 
	{
		if (!isOpen)
		{
			throw new NoSuchElementException("this DbFileIterator is closed");
		}
		Tuple nextTuple =  currentTupleIter.next();
		if (!currentTupleIter.hasNext())
		{
			// read next page
			if (currentPageId.pageno() < heapFile.numPages() - 1)
			{
				currentPageId = new HeapPageId(heapFile.getId(), currentPageId.pageno() + 1);
				HeapPage page = (HeapPage) pool.getPage(tid, currentPageId, Permissions.READ_ONLY,null);
				currentTupleIter = page.iterator();
			}
		}
		return nextTuple;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException 
	{
		if (!isOpen)
		{
			throw new NoSuchElementException("this DbFileIterator is closed");
		}
		currentPageId = new HeapPageId(heapFile.getId(), 0);
		HeapPage page = (HeapPage) pool.getPage(tid, currentPageId, Permissions.READ_ONLY,null);
		currentTupleIter = page.iterator();
	}

	@Override
	public void close() {
		isOpen = false;
	}

}

package simpledb.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.Permissions;
import simpledb.RecordId;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.page.HeapPage;
import simpledb.page.HeapPageId;
import simpledb.page.Page;
import simpledb.page.PageId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor. commited to github - test
 * 
 * @see simpledb.page.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File fileOnDisk;
	private TupleDesc tupleDesc;
	private int tableId;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		fileOnDisk = f;
		tupleDesc = td;
		tableId = f.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return fileOnDisk;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return tableId;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) 
	{
		Page result = null;
		RandomAccessFile randomAcc = null;
		byte[] bPage = new byte[BufferPool.PAGE_SIZE];
		try 
		{
			randomAcc = new RandomAccessFile(fileOnDisk, "r");
			int pageNum = pid.pageno();
			randomAcc.seek(pageNum * BufferPool.PAGE_SIZE);
			randomAcc.read(bPage);
			result = new HeapPage(pid, bPage);
		} 
		catch (IOException e) 
		{

		} 
		finally 
		{
			if (randomAcc != null) {
				closeRandomFile(randomAcc);
			}
		}
		return result;
	}

	private void closeRandomFile(RandomAccessFile randomAcc) {

		try {
			if (randomAcc != null) {
				randomAcc.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException 
	{
		int offset = page.getId().pageno();
		byte[] data = page.getPageData();
		RandomAccessFile randomAcc = null;
		try 
		{
			randomAcc = new RandomAccessFile(fileOnDisk, "rw");
			randomAcc.seek(offset * BufferPool.PAGE_SIZE);
			randomAcc.write(data);
		} 
		catch (IOException e) 
		{

		}
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) Math.floor(fileOnDisk.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple tuple) throws DbException, IOException, TransactionAbortedException 
	{
		BufferPool bufferPool = Database.getBufferPool();
		ArrayList<Page> pages = new ArrayList<Page>();
		boolean needNewPage = true;
		//assert(tuple.getRecordId() == null);
		// check if there is empty slot.
		for (int i = 0; i < numPages(); i++) 
		{
			PageId pid = new HeapPageId(getId(), i);
			HeapPage page = (HeapPage)bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
			if (page.getNumEmptySlots() > 0)
			{
				needNewPage = false;
				pages = addTupleToPage(tuple, page);
				break;
			}
		}
		// all the slots are full.
		if (needNewPage)
		{
			// add extra page.
			byte[] data = new byte[BufferPool.PAGE_SIZE];
			PageId pid = addEmptyPage(data);
			assert numPages() > 0;
			// add the tuple to the new page
			HeapPage newPage = new HeapPage(pid, data);
			pages = addTupleToPage(tuple, newPage);
		}
		return pages;
	}

	private PageId addEmptyPage(byte[] data) throws IOException
	{
		FileOutputStream fos = null;
		int lastPageNum = numPages() - 1;
		try
		{
		fos = new FileOutputStream(fileOnDisk, true);
		fos.write(data);
		return new HeapPageId(tableId, lastPageNum + 1);
		}
		finally
		{
			if(fos != null)
			{
				fos.close();
			}
		}
	}

	private ArrayList<Page> addTupleToPage(Tuple t, Page p) throws DbException, IOException {
		ArrayList<Page> pages = new ArrayList<Page>();
		((HeapPage)p).addTuple(t);
		pages.add(p);
		return pages;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,TransactionAbortedException 
	{
		RecordId record = t.getRecordId();
		PageId pageId = record.getPageId();
		BufferPool bufferPool = Database.getBufferPool();
		HeapPage p = null;
		p = (HeapPage)bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
		p.deleteTuple(t);
		return p;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(this, tid);
	}
}

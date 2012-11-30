package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.DbFile;
import simpledb.locking.DbLock;
import simpledb.locking.LockManager;
import simpledb.page.Page;
import simpledb.page.PageId;
import simpledb.tuple.Tuple;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096; // 4k
    public Object lock = new Object();
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private Map<PageId, Page> bufferedPages = new HashMap<PageId, Page>();
    private List<PageId> queue;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) 
    {
    	this.numPages = numPages;
    	queue = new ArrayList<PageId>(numPages);
    	LockManager.getInstance().reset();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
    	try 
    	{
    		LockManager lockManager = LockManager.getInstance();
    		DbLock dbLock = lockManager.getLock(pid);
    		synchronized (dbLock) 
    		{
    			lockManager.addLockRequest(tid, pid);
	    		// try to get lock on the page 
				dbLock.lock(tid, perm);
	    		// notify about the success to lock page.
	    		lockManager.addLockedPage(tid, pid);
    		}
    		
    		Page bufferedPage = bufferedPages.get(pid);
			if (bufferedPage == null)
			{
				DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
				bufferedPage = dbFile.readPage(pid);
				bufferedPage.markDirty(false, null);
			}
			if (isEvicationRequered())
			{
				evictPage();
			}
			addToQueue(pid);				
			bufferedPages.put(pid, bufferedPage);
			return bufferedPage;
		} 
    	catch (NoSuchElementException e) 
		{
			throw new DbException(e.getMessage());
		}
        
    }
	
	private boolean isEvicationRequered() 
	{
		
		return queue.size() == numPages;
	}

	private void addToQueue(PageId pid) {
		if (!queue.contains(pid))
		{
			queue.add(pid);
		}
		else
		{
			queue.remove(pid);
			queue.add(pid);
		}
	}

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) 
    {
    	LockManager lm = LockManager.getInstance();
    	DbLock dbLock = lm.getLock(pid);
		synchronized (dbLock) 
		{
			dbLock.unlock(tid);
		}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException 
    {
    	transactionComplete(tid, true);
    }

	

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) 
    {
    	LockManager lm = LockManager.getInstance();
    	return lm.getLock(p).hasLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException 
    {
    	LockManager lockManager = LockManager.getInstance();
    	Set<PageId> lockedPages = lockManager.getLockedPages(tid);
    	for (PageId p : lockedPages) 
		{
    		DbLock dbLock = lockManager.getLock(p);
	    	if (commit)
	    	{
	    		// use current page contents as the before-image for the next transaction that modifies this page. 
	    		Page page = bufferedPages.get(p);
	    		page.setBeforeImage();
	    		flushPage(p);
	    	}
	    	else
	    	{
	    		restorePage(p);
	    	}
	    	// notify Lock Manager that this page not locked anymore by this pid.
	    	LockManager.getInstance().removeLockedPage(tid, p);
	    	
	    	synchronized (dbLock) 
	    	{
	    		// unlock the page
	    		dbLock.unlock(tid);
			}
		}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException 
    {
    	Catalog catalog = Database.getCatalog();
    	DbFile dbFile = catalog.getDbFile(tableId);
    	ArrayList<Page> pages = dbFile.addTuple(tid, t);
    	Page page = pages.get(0);
    	// mark dirty
    	page.markDirty(true, tid);
    	// a possible new page was created during the insertion
    	bufferedPages.put(page.getId(), page);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, int tableId, Tuple t) throws DbException, TransactionAbortedException 
    {
    	Catalog catalog = Database.getCatalog();
    	DbFile dbFile = catalog.getDbFile(tableId);
    	Page deletedTuplePage = dbFile.deleteTuple(tid, t);
    	// mark dirty
    	deletedTuplePage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException 
    {
    	Set<PageId> keySet = bufferedPages.keySet();
    	for (PageId pageId : keySet) 
    	{
    		flushPage(pageId);
		}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    public synchronized  void flushPage(PageId pid) throws IOException
    {
    	Page p = bufferedPages.get(pid);
    	
    	if (p != null && p.isDirty() != null)
    	{
			DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
			// append an update record to the log, with a before-image and after-image. 
			TransactionId dirtier = p.isDirty();
			if (dirtier != null)
			{ 
				Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p); 
				Database.getLogFile().force(); 
			}
			dbFile.writePage(p);
			p.markDirty(false, null);
		}
    }
    
    public synchronized  void restorePage(PageId pid) throws IOException
    {
    	Page p = bufferedPages.get(pid);
    	
    	if (p != null && p.isDirty() != null)
    	{
			DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
			p = dbFile.readPage(pid);
			p.markDirty(false, null);
			bufferedPages.put(pid, p);
		}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException 
    {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @param tid 
     * @param pid 
     * @throws TransactionAbortedException 
     * @throws IOException 
     */
    private synchronized  void evictPage() throws DbException
    {
        boolean pageEvicted = false;
        //LockManager lm = LockManager.getInstance();
        try 
        {
        	for (PageId nextId : queue) 
        	{
        		// try evict the page if not dirty.
        		if (tryEvict(nextId))
        		{
        			//lm.evictPage(nextId);
        			pageEvicted = true;
        			break;
        		}
        		
			}
	        if (!pageEvicted)
	        {
	        	throw new DbException("evacation error : all pages in the buffer pool are dirty");
	        }
		} 
        catch (IOException e) 
        {
			Debug.log("error accured on flushing the page to disk: " + e, "%s");
		}
    }

	private boolean tryEvict(PageId nextId) throws IOException
	{
		Page pageToFlush = bufferedPages.get(nextId);
		// must never evict a dirty page
		if (pageToFlush.isDirty() == null)
    	{
			bufferedPages.remove(nextId);
    		queue.remove(nextId);
    		return true;
    	}
		return false;
	}

}

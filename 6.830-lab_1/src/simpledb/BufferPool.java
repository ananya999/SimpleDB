package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.DbFile;
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

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private Map<PageId, Page> bufferedPages = new HashMap<PageId, Page>();
    private List<PageId> queue;
    
    private Map <TransactionId, HashSet<PageId>> m_tarnsactionPage = new HashMap <TransactionId, HashSet<PageId>>();
    private Map<PageId, ReentrantReadWriteLock> m_pageLock= new HashMap<PageId, ReentrantReadWriteLock>();

    private ReadLockCounter pageReadLockCouter;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) 
    {
    	this.numPages = numPages;
    	queue = new ArrayList<PageId>(numPages);
    	pageReadLockCouter = new ReadLockCounter();
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
    	try 
    	{
    		ReentrantReadWriteLock lock = getPageLock(tid, pid);
    		if (perm == Permissions.READ_ONLY)
    		{
    			lock.readLock().lock();
    			pageReadLockCouter.addTransaction(pid, tid);
    		}
    		else if (perm == Permissions.READ_WRITE)
    		{
    			// upgrade lock on page to an exclusive lock.
    			if (pageReadLockCouter.isUpgradeLockPermited(pid, tid))
    			{
    				lock.readLock().unlock();
    			}
    			lock.writeLock().lock();
    		}
			
    		Page bufferedPage = bufferedPages.get(pid);
			if (bufferedPage == null)
			{
				DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
				bufferedPage = dbFile.readPage(pid);
				bufferedPage.markDirty(false, null);
			}
			updateQueue(pid);				
			bufferedPages.put(pid, bufferedPage);
			if (bufferedPages.size() > numPages)
			{
				evictPage();
			}
			return bufferedPage;
		} 
    	catch (NoSuchElementException e) 
		{
			throw new DbException(e.getMessage());
		}
        
    }

	private ReentrantReadWriteLock getPageLock(TransactionId tid, PageId pid) 
	{
		HashSet<PageId> transactionlockedPages = m_tarnsactionPage.get(tid);
		if (transactionlockedPages == null)
		{
			transactionlockedPages = new HashSet<PageId>();
			m_tarnsactionPage.put(tid, transactionlockedPages);
		}
		transactionlockedPages.add(pid);
		
		ReentrantReadWriteLock lock = m_pageLock.get(pid);
		if (lock == null)
		{
			lock = new ReentrantReadWriteLock();
			
			m_pageLock.put(pid, lock);
			
		}
		return lock;
	}
	
	private void updateQueue(PageId pid) {
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
    	// unlock the page.
    	ReentrantReadWriteLock lock = m_pageLock.get(pid);
    	if (lock.getReadLockCount() > 0)
    	{
    		lock.readLock().unlock();
    	}
    	if (lock.getWriteHoldCount() > 0)
    	{
    		lock.writeLock().unlock();
    	}
		// remove the page from the transaction.
    	HashSet<PageId> pages = m_tarnsactionPage.get(tid);
    	if (pages != null)
    	{
    		pages.remove(pid);
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

	private Set<PageId> releaseAllLockedPages(TransactionId tid) 
	{
		HashSet<PageId> pages = m_tarnsactionPage.get(tid);
    	for (PageId id : pages) 
    	{
    		ReadWriteLock lock = m_pageLock.get(id);
    		lock.readLock().unlock();
    		lock.writeLock().unlock();
		}
    	return pages;
	}

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) 
    {
    	HashSet<PageId> pages = m_tarnsactionPage.get(tid);
    	for (PageId id : pages) 
    	{
    		if (id.equals(p))
    		{
    			return true;
    		}
    	}
    	return false;
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
    	if (commit)
    	{
    		HashSet<PageId> lockedPages = m_tarnsactionPage.get(tid);
    		for (PageId id : lockedPages) 
    		{
    			flushPage(id);
			}
    		// releasing any locks that the transaction held.
    		releaseAllLockedPages(tid);
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
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

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
			dbFile.writePage(p);
			p.markDirty(false, null);
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
    	try 
        {
        	for (PageId nextId : queue) 
        	{
        		// try evict the page if not dirty.
        		if (tryEvict(nextId))
        		{
        			pageEvicted = true;
        		}
        		
			}
	        if (!pageEvicted)
	        {
	        	throw new DbException("all pages in the buffer pool are dirty");
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

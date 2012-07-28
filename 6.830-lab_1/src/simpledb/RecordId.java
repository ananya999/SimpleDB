package simpledb;

import simpledb.page.PageId;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

	private PageId m_pid;
	private int m_tupleno;
	
    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) 
    {
        m_pid = pid;
        m_tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() 
    {
        return m_tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return m_pid;
    }
    
    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecordId other = (RecordId) obj;
		if (m_pid == null) {
			if (other.m_pid != null)
				return false;
		} else if (!m_pid.equals(other.m_pid))
			return false;
		if (m_tupleno != other.m_tupleno)
			return false;
		return true;
	}
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_pid == null) ? 0 : m_pid.hashCode());
		result = prime * result + m_tupleno;
		return result;
	}
    
}

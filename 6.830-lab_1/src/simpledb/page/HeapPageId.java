package simpledb.page;


/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

	private int m_tableId;
	private int m_pgNo;
	
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) 
    {
    	m_tableId = tableId;
    	m_pgNo = pgNo;
    	
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return m_tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageno() {
        return m_pgNo;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_pgNo;
		result = prime * result + m_tableId;
		return result;
	}

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HeapPageId other = (HeapPageId) obj;
		if (m_pgNo != other.m_pgNo)
			return false;
		if (m_tableId != other.m_tableId)
			return false;
		return true;
	}

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageno();

        return data;
    }

	@Override
	public String toString() {
		return "HeapPageId [m_tableId=" + m_tableId + ", m_pgNo=" + m_pgNo
				+ "]";
	}

}

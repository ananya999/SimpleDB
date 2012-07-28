package simpledb.systemtest;

import java.io.IOException;
import static org.junit.Assert.*;
import simpledb.*;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.HeapFile;
import simpledb.operators.Filter;
import simpledb.operators.SeqScan;
import simpledb.predicates.Predicate;

public class FilterTest extends FilterBase {
    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException, IOException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        filter.open();

        int resultCount = 0;
        while (filter.hasNext()) {
            assertNotNull(filter.next());
            resultCount += 1;
        }

        filter.close();
        return resultCount;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(FilterTest.class);
    }
}

package simpledb;

import java.io.File;

import simpledb.file.HeapFile;
import simpledb.operators.SeqScan;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

public class Test {
	
	public static void main(String[] args) 
	{
		Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
		String[] names = new String[]{"field0", "field1", "field2"};
		TupleDesc descriptor = new TupleDesc(types, names);
		
		HeapFile table1 = new HeapFile(new File("C:\\workspace\\DB\\6.830-lab1\\dist\\new_3.dat"), descriptor);
		Database.getCatalog().addTable(table1, "test");
		
		TransactionId tid = new TransactionId();
		SeqScan f = new SeqScan(tid, table1.getId(), "test");
		
		try
		{
			f.open();
			while (f.hasNext())
			{
				Tuple t = f.next();
				System.out.print(t);
			}
			Database.getBufferPool().transactionComplete(tid);
		}
		catch (Exception e) 
		{
			System.out.println("Exception : " + e);
		}
		finally
		{
			f.close();
		}
		
	}
}

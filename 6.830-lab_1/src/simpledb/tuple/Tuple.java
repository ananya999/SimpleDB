package simpledb.tuple;

import org.apache.commons.lang3.ArrayUtils;

import simpledb.RecordId;


/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

	private TupleDesc tupleDesc;
	private Field[] fieldsArr;
	RecordId recordId = null;
	
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) 
    {
        tupleDesc = td;
        fieldsArr = new Field[td.numFields()];
    }

    public static Tuple combine(Tuple t1, Tuple t2) 
    {
    	Tuple combinedt = null;
    	TupleDesc combinedtd = TupleDesc.combine(t1.tupleDesc, t2.tupleDesc);
    	Field[] combinedf = ArrayUtils.addAll(t1.fieldsArr, t2.fieldsArr);
    	combinedt = new Tuple(combinedtd);
    	combinedt.fieldsArr = combinedf;
    	return combinedt;
    }
    
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() 
    {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) 
    {
    	recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) 
    {
    	fieldsArr[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldsArr[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() 
    {
    	String res = "";
    	StringBuilder builder = new StringBuilder();
    	if (fieldsArr != null && fieldsArr.length > 0)
    	{
    		for (Field field : fieldsArr) 
    		{
    			builder.append(field.toString()).append("\t");
			}
    		res =  builder.toString();
    		res = res.replaceAll("\t$", "\n");
    	}
    	return res;
    }
}

package simpledb.tuple;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;


/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	
	private Type[] types;
	private String[] fieldsName;
	private String alias = null;
	

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) 
    {
    	String[] combineNames  = ArrayUtils.addAll(td1.fieldsName, td2.fieldsName);
    	Type[] combineTypes = ArrayUtils.addAll(td1.types, td2.types);
    	TupleDesc combinedtd = new TupleDesc(combineTypes, combineNames);
    	return combinedtd;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) 
    {
    	types = typeAr;
    	fieldsName = fieldAr;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) 
    {
    	types = typeAr;
    }

    /**
     * copy Constructor
     * @param tupleDesc
     */
    public TupleDesc(TupleDesc tupleDesc) 
    {
		types = (Type[])tupleDesc.types.clone();
		if (tupleDesc.fieldsName != null)
		{
			fieldsName = (String[])tupleDesc.fieldsName.clone();
		}
	}

	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() 
    {
        return types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (fieldsName == null)
        {
        	return "null";
        }
    	if (fieldsName.length <= i)
        {
        	throw new NoSuchElementException(i + " is not a valid field reference.");
        }
    	return fieldsName[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException 
    {
    	int index = 0;
    	boolean found = false;
    	if (fieldsName == null)
    	{
    		throw new NoSuchElementException();
    	}
    	for (int i = 0; !found && i < fieldsName.length; i++) 
    	{
    		if (ignoreTableNameEquels(fieldsName[i],name));
			{
				index = i;
				found = true;
			}
		}
    	if (!found)
    	{
    		throw new NoSuchElementException();
    	}
    	return index;
    }

    private boolean ignoreTableNameEquels(String tdName, String fName) 
    {
		String tdSubstring = tdName.substring(tdName.indexOf('.') + 1);
		String fSubstring = fName.substring(fName.indexOf('.') + 1);
    	return tdSubstring.equals(fSubstring);
	}

	/**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException 
    {
        return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
    	for (Type type : types) 
        {
        	size = size + type.getLen();
		}
    	return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (this == o)
        {
        	return true;
        }
        if (!(o instanceof TupleDesc))
        {
        	return false;
        }
        TupleDesc t = (TupleDesc) o;
        return Arrays.equals(t.types, this.types);
    }

    public int hashCode() 
    {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() 
    {
        // some code goes here
        return "";
    }
    public void setAliasToFields(String as)
    {
    	if (alias == null || !alias.equals(as))
    	{
	    	alias = as;
    		for (int i = 0; fieldsName != null && i < fieldsName.length; i++) 
	    	{
	    		fieldsName[i] = as + "." + fieldsName[i];
			}
    	}
    }
}

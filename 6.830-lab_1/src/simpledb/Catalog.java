package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import simpledb.file.DbFile;
import simpledb.file.HeapFile;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

	private Map<Integer, TableProperties> idToTableProperties;
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() 
    {
    	idToTableProperties = new HashMap<Integer, TableProperties>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) 
    {
    	idToTableProperties.put(file.getId(), new TableProperties(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file,name,"");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     */
    /*public void addTable(DbFile file) {
        addTable(file, (new UUID()).toString());
    }*/

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) {
        int result = 0;
        boolean found = false;
    	Set<Integer> idKey= idToTableProperties.keySet();
        for (Integer id : idKey) 
        {
        	if (idToTableProperties.get(id).getTableName().equalsIgnoreCase(name))
        	{
        		result = id;
        		found = true;
        		break;
        	}
		}
        if (!found)
        {
        	throw new NoSuchElementException();
        }
        return result;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        return idToTableProperties.get(tableid).getFile().getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableId) throws NoSuchElementException 
    {
        DbFile file = idToTableProperties.get(tableId).getFile();
        if (file == null)
        {
        	throw new NoSuchElementException("DB file not found for " + tableId);
        }
		return file;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        idToTableProperties.clear();
    }

    public String getPrimaryKey(int tableid) {
        
        return idToTableProperties.get(tableid).getPk();
    }

    public Iterator<Integer> tableIdIterator() {
        
        return idToTableProperties.keySet().iterator();
    }

    public String getTableName(int id) {
        return idToTableProperties.get(id).getTableName();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
    
    private class TableProperties 
    {
    	DbFile file;
    	String tableName;
    	String pk;
    	
    	public TableProperties(DbFile file, String tableName, String pk) 
    	{
			this.file = file;
			this.tableName = tableName;
			this.pk = pk;
		}

		public DbFile getFile() 
		{
			return file;
		}
		
		public String getTableName() 
		{
			return tableName;
		}
		
		public String getPk() 
		{
			return pk;
		}
    }
}


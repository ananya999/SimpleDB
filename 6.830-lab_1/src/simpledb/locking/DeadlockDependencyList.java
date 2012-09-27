package simpledb.locking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import simpledb.TransactionId;
import simpledb.locking.graph.Edge;
import simpledb.locking.graph.Graph;
import simpledb.locking.graph.Vertex;


/**
 * @author felix
 * 
 * implement "waits-for graph" to detect deadlock cycles. 
 * The nodes correspond to active transactions, 
 * and there is an arc from Ti to 
 * Tj if (and only if) Ti is waiting for Tj to release a lock.
 * 
 * Facade class
 *
 */

public class DeadlockDependencyList 
{
	
	private Graph<TransactionId> graph;
	
	
	public DeadlockDependencyList() 
	{
		graph = new Graph<TransactionId>();
	}
	
	public void addEdge(TransactionId fromTid , TransactionId toTid)
	{
		Vertex<TransactionId> f_vertex = graph.findVertexByName(fromTid.toString());
		if(f_vertex == null)
		{
			f_vertex = new Vertex<TransactionId>(fromTid.toString(), fromTid);
			graph.addVertex(f_vertex);
		}
		
		Vertex<TransactionId> t_vertex = graph.findVertexByName(toTid.toString());
		if(t_vertex == null)
		{
			t_vertex = new Vertex<TransactionId>(toTid.toString(), toTid);
			graph.addVertex(t_vertex);
		}
		graph.addEdge(f_vertex, t_vertex, 0);
	}
	
	public void removeEdge(TransactionId fromTid , TransactionId toTid)
	{
		Vertex<TransactionId> f_vertex = graph.findVertexByName(fromTid.toString());
		Vertex<TransactionId> t_vertex = graph.findVertexByName(toTid.toString());
		graph.removeEdge(f_vertex, t_vertex);
	}
	
	public void removeTransaction(TransactionId tid)
	{
		Vertex<TransactionId> v = graph.findVertexByName(tid.toString());
		graph.removeVertex(v);
	}
	
	public Set<TransactionId> getConflictingTransactions()
	{
		Set<TransactionId> result = new HashSet<>();
		Edge<TransactionId>[] findCycles = graph.findCycles();
		for (Edge<TransactionId> e : findCycles) 
		{
			result.add(e.getFrom().getData());
		}
		return result;
	}
	
	public void reset()
	{
		graph = new Graph<TransactionId>();
	}
}

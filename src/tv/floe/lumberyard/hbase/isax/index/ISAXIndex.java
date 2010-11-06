package tv.floe.lumberyard.hbase.isax.index;

import java.io.IOException;
import java.util.Iterator;

import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.dev.Symbol;
import edu.hawaii.jmotif.datatype.isax.index.HashTreeException;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.InternalNode;
import edu.hawaii.jmotif.datatype.isax.index.NodeType;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;

/**
 *	-	how do we want to reference a table now? 
 *		-	need to look at ways to set it at query time yet not propogate the string everywhere
 *
 * -	does this run as a service that pulls conf files each query?
 * 
 * -	need to think about things in terms of client lib 
 * 		-	and then in terms as running tornado/jetty service, conf vs command params
 * 
 *  	-	what is base case?
 * 
 * @author jpatterson
 *
 */
public class ISAXIndex implements Iterable<NodePersisted> {
	
	// ----- need to rethink how we pull this
	final public static String ROOT_NODE_KEY = "root_node_00";
	final public static String COLUMN_FAMILY_NAME = "node";
	final public static String STORE_COL_NAME = "store";

	//IndexHashParams params;
	InternalNodePersisted root_node = null;
	
	// need to have a non-static index specific value here, could be working with multiple 
	private String hbase_table_name = "";
	
	/*
	 * CTOR ------------- need to work out how these hardcoded params are going to be loaded dynamically depending on table
	 */
	public ISAXIndex() {
		
	//	this.hbase_table_name = hbase_table_name;
	  
	// we could set these params in a conf.xml file
/*	  params = new IndexHashParams();
	  params.base_card = 4;
	  params.d = 1;
	  params.isax_word_length = 4;
	  params.orig_ts_len = 8;
	  params.threshold = 100;
*/	
	  //Sequence s = new Sequence( 8 ); // root node seqeunce, needs nothing more than a word len
		
	  //Sequence s = new Sequence( params.orig_ts_len ); // root node seqeunce, needs nothing more than a word len
	
//	  this.LoadRoot();
	  
//		this.root_node.DebugKeys();
		
		
	}
	
	public boolean LoadIndex( String table_name ) {
		
		this.hbase_table_name = table_name;
		
		// does the table exist?
		
		
		// pull the root node
		
		// root node must exist because thats where the params is serialized
		
		try {
			this.LoadRoot();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if ( null == this.root_node ) {
			
			return false;
			
		}
		
		//return null;
		return true;
	}
	
	private void LoadRoot() throws Exception {
		
		  
		byte[] node_bytes = null;
		try {
			node_bytes = HBaseUtils.Get( ROOT_NODE_KEY, this.hbase_table_name, ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
		if ( null == node_bytes ) {

			// insert root
/*
			  Sequence root_key = new Sequence( params.orig_ts_len ); // root node seqeunce, needs nothing more than a word len
			  
			  this.root_node = new InternalNodePersisted( root_key, params, NodeType.ROOT );
			
			
			byte[] bytes_node = this.root_node.getBytes();
			
			try {
				HBaseUtils.Put( ROOT_NODE_KEY, this.hbase_table_name, "node", "store", bytes_node );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println( "\n\nISAXIndex > No Root Found, Inserting Stock Root!" );
*/
			throw new Exception( "No root found in storage for table " + this.hbase_table_name );
			
			
		} else {
		
			System.out.println( "\n\nISAXIndex > Found Root! > " + node_bytes.length + " bytes" );

			this.root_node = new InternalNodePersisted( );
			  this.root_node.key = new Sequence( 0 );
			  //this.root_node.key.parseFromIndexHash( sax_hash_key );
			
			this.root_node.deserialize(node_bytes);
		
			this.root_node.key.setOrigLength(this.root_node.params.orig_ts_len);
		
			this.root_node.SetTableName(hbase_table_name);
			//serde_node.DebugInstances();
		
		}
				
		
		
	}
	
	public static boolean DoesIndexExist( String table_name ) {
		
		// check for the existence of this table
		
		
		//return ISAXIndex.DoesIndexExist( table_name );
		try {
			return HBaseUtils.DoesTableExist(table_name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	public static boolean CreateNewIndex( String table_name, String strBaseCard, String strDimPerSplit, String base_word_len, String base_ts_sample_size, String split_threshold ) {

		/*	  params = new IndexHashParams();
		  params.base_card = 4;
		  params.d = 1;
		  params.isax_word_length = 4;
		  params.orig_ts_len = 8;
		  params.threshold = 100;
	*/			
		
		int base_cardinality = Integer.parseInt(strBaseCard);
		int dimensionality = Integer.parseInt(strDimPerSplit);
		int word_len = Integer.parseInt(base_word_len);
		int ts_len = Integer.parseInt(base_ts_sample_size);
		int th = Integer.parseInt(split_threshold);
		
		IndexHashParams p = new IndexHashParams();
		p.base_card = base_cardinality;
		p.d = dimensionality;
		p.isax_word_length = word_len;
		p.orig_ts_len = ts_len;
		p.threshold = th;
		
		return CreateNewIndex( table_name, p );
		
		
	}
	
	public static boolean CreateNewIndex( String table_name, IndexHashParams p ) {
		
		
		boolean bCreated = false;
		
		System.out.println( "Lumberyard > CreateTable > " + table_name );
		
		// let's see if this table_name already exists
		
		if ( ISAXIndex.DoesIndexExist( table_name ) ) {
			
			// index already exists, do nothing
			
			// should we not throw an exception here?
			
			System.out.println( "Table already exists, quitting..." );
			
			return false;
			
		} else {
			
			
			
		}
		
		// if index does not exit, create in hbase
		
		try {
			HBaseUtils.CreateNewTable( table_name );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		// now create a root node and serialize this to hbase

		
		// its key to have the root since this is where the core params live that define the resolution of the table
		

		Sequence root_key = new Sequence( p.orig_ts_len ); // root node seqeunce, needs nothing more than a word len
		  
		InternalNodePersisted root_node = new InternalNodePersisted( root_key, p, NodeType.ROOT );
		
		
		byte[] bytes_node = root_node.getBytes();
		
		// need to use custom table name, col family, col name, etc
		try {
			HBaseUtils.Put( ROOT_NODE_KEY, table_name, ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, bytes_node );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println( "Table Created!\n" );
		
		return true;
		
	}
	
	public static boolean DeleteIndex( String table_name ) {
		
		System.out.println( "Lumberyard > Delete Index: " + table_name );
		
		if ( ISAXIndex.DoesIndexExist( table_name ) ) {
			
			try {
				HBaseUtils.DropTable(table_name);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			
		} else {
			
			System.out.println( "\tIndex " + table_name + " does not exit, quitting..." );
			return false;
		}		
		

		return true; //ISAXIndex.DeleteIndex(table_name);
		
	
	}
	
	public static void DebugIndex( String table_name ) {
		
		// let's see if this table_name already exists
		
		if ( ISAXIndex.DoesIndexExist( table_name ) ) {
			
			// index already exists, do nothing
		
			System.out.println( "Table for index '" + table_name + "' exists." );
			
		} else {
			
			System.out.println( "Table for index '" + table_name + "' does NOT exists." );
			
		}		
		
		ISAXIndex index = new ISAXIndex();
		index.LoadIndex(table_name);
		index.DebugRootNode();

		// what does the root node tell us about this index?
		
		
		
		// at some point we want to be able to iterate through all of the nodes in the index
		
	}
	
	public void DebugRootNode() {
		
		System.out.println( "Root Node For: " + this.hbase_table_name );
		System.out.println( "\tTable Name: " + this.root_node.GetTableName() );
		System.out.println( "\tKey: " + this.root_node.key.getIndexHash() );
		System.out.println( "\tBase Card: " + this.root_node.params.base_card );
		System.out.println( "\tDim Per Split: " + this.root_node.params.d );
		System.out.println( "\tSplit Current Dim Index: " + this.root_node.params.dim_index );
		System.out.println( "\tISAX Word Length: " + this.root_node.params.isax_word_length );
		System.out.println( "\tOrig TS Length: " + this.root_node.params.orig_ts_len );
		System.out.println( "\tThreshold: " + this.root_node.params.threshold );
		
		
	}
	
	
	
	/*
	 * This is the basic timeseries insert function for the hbase-based isax index
	 */
	  public void InsertSequence( Timeseries ts, String filename, long offset ) {
		  
		  TimeseriesInstance ts_inst = new TimeseriesInstance( ts );
		  
		  ts_inst.AddOccurence(filename, offset);
		  try {
			  this.InsertSequence( ts_inst );
		  } catch (HashTreeException e) {
			  
			  System.out.println( "Exception: " + e );
			  System.out.println( "ts: " + ts.toString() );
			  System.out.println( "ts: " + ts_inst.getTS().toString() );
			  
		  }
	  }
	  
	  public void InsertSequence( TimeseriesInstance ts_inst ) throws HashTreeException {

	//	  this.root_node.Insert( ts_inst );
		
		  // create a root node

		  
		  //InternalNode root_node = new InternalNode( new Sequence( this.params.orig_ts_len ), this.params, NodeType.ROOT );
				  
		  if ( null == this.root_node ) {
			  
			  System.out.println( "ISAXIndex > InsertSequence > ERROR > Can't find Root Node!" );
			  
		  } else {
			  
			  System.out.println( "Found root, inserting instance..." );
			  
			  this.root_node.Insert(ts_inst);
			  
			  
		  }
		  
		  
		  
	  }


	  /**
	   * 
	   * Approximate search for timeseries ts
	   * 
	   * Used to find all of the occurences of ts.
	   * 
	   * @param ts
	   * @return a TimeseriesInstance object with filenames and offset positions.
	   */
	  public TimeseriesInstance ApproxSearch( Timeseries ts ) {
		  
		  return this.root_node.ApproxSearch(ts);
		  
	  }
		
	/*
	 * This function is meant to index a shape into a timeseries pattern
	 * 
	 */
	public void InsertShape() {
		
		
	}
	
	
	/**
	 * 
	 * Insert a smaller DNA strand
	 * 
	 * 
	 */
	  public void InsertDNAString( String dna_sample, int window_len, String source_name ) {
			 
		  String dna_window = "";
		  int curr_offset = 0;
		  int beginIndex = 0;
		  int endIndex = 0;
			
			while ( curr_offset < dna_sample.length() ) {
				
				beginIndex = curr_offset;
				if (beginIndex + window_len < dna_sample.length() ) {
					
					endIndex = beginIndex + window_len;
					
				} else {
					
					endIndex = dna_sample.length();
					
				}
				
				if ( endIndex >= dna_sample.length() ) {
					break;
				}
			
				dna_window = dna_sample.substring(beginIndex, endIndex);
				Timeseries ts_dna = null;
				
				try {
					ts_dna = ISAXUtils.CreateTimeseriesFromDNA( dna_window );
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
							
				this.InsertSequence(ts_dna, source_name, beginIndex );
					
				curr_offset = endIndex;
				
				
				
			}	  
		  
	  }
	
	/*
	 * Inser an entire genome
	 */
	public void InsertGenome() { 
		
		// give the index a local file reference?
		
	}
	
	
	/*
	 * 
	 */
	public void InsertWaveform() {
		
		
	}

	@Override
	public Iterator<NodePersisted> iterator() {
		// TODO Auto-generated method stub
		return null;
	}
	
}

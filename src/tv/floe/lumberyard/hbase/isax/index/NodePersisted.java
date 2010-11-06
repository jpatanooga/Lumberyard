package tv.floe.lumberyard.hbase.isax.index;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.index.AbstractNode;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.InternalNode;
import edu.hawaii.jmotif.datatype.isax.index.NodeType;
import edu.hawaii.jmotif.datatype.isax.index.SerDeUtils;


/**
 * Base class to handle SerDe mechanics for nodes into/out of hbase
 * 
 * 
 * 
 * ToDo
 * 
 * 	-	need to examine common Hadoop SerDe techniques, want to maximize reuse
 * 
 *  	-	what structures do we need to serialize?
 *  
 *  		-	InternalNode
 *  
 *  			-	Node Type
 *  			-	IndexHashParams
 *  			-	descendants (array of key strings)
 *  
 *  
 *  		-	Terminal Node
 *  
 *  			-	Node Type
 *  			-	IndexHashParams
 *  			-	Instances (array of objects)
 *  			
 *  	
 * 
 *  - how do we wire into hbase? look at Sparky code
 * 
 */
public class NodePersisted extends AbstractNode {

	//String key = "";
	private String hbase_table_name = "";
	
	public NodePersisted() {
		
		//this.key = k;
		
	}
	
	public String GetTableName() {
		return this.hbase_table_name;
	}
	
	public void SetTableName(String table_name) {
		this.hbase_table_name = table_name;
	}
	
	public void SetKeySequence( Sequence sax ) {
		this.key = sax;
	}
	
	/**
	 * Load the object from HBase
	 * - rememmber to pull the orig length from the hash params
	 * 
	 * 
	 * @param sax_hash_key
	 */
	public static NodePersisted LoadFromStore( String isax_hash, String table_name ) {
		
		  NodePersisted hbase_node = null;
			byte[] node_bytes = null;
			try {
				node_bytes = HBaseUtils.Get( isax_hash, table_name, ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
			if (null == node_bytes) {
				return null;
			}
			
			hbase_node = NodePersisted.deserialize_unknown(isax_hash, node_bytes);
		
			hbase_node.SetTableName(table_name);
		
		return hbase_node;
		
	}
	
	public byte[] getBytes() {
		
		
		
		
		return null;
		
	}
	
	public void WriteToStore() {
		
		// take the fields and write into hbase
		
		
	}
	
	
	
	  public static NodePersisted deserialize_unknown(String isax_hash, byte[] src_bytes) {
		  
		  //NodePersisted out_node = null;
		  
		  int nt_d = SerDeUtils.byteArrayToInt(src_bytes, 0);
		  
		  if ( 0 == nt_d ) {
			  //this.setType(NodeType.ROOT);
			  
			  System.out.println( "NodePersisted > Deserialize > Should not be SerDe'ing a Root Node > ERROR" );
			  
		  } else if (1 == nt_d) {
			  
			  InternalNodePersisted out_node = new InternalNodePersisted();
			  
			  out_node.setType(NodeType.INTERNAL);
			  
			  out_node.key = new Sequence( 0 );
			  out_node.key.parseFromIndexHash( isax_hash );
			  
			  out_node.deserialize(src_bytes);
				
			  out_node.key.setOrigLength( out_node.params.orig_ts_len);
			  

				//out_node.key = new Sequence( 0 );
				
				//hbase_node.key.setOrigLength( hbase_node.params.orig_ts_len );
			  
			  
			  return out_node;
			  
		  } else if (2 == nt_d) {
	
			  TerminalNodePersisted out_node = new TerminalNodePersisted( new Sequence( 0 ) );
			  
			  out_node.setType(NodeType.TERMINAL);
			  out_node.key = new Sequence( 0 );
			  out_node.key.parseFromIndexHash( isax_hash );

			  out_node.deserialize(src_bytes);
			  
				
			  out_node.key.setOrigLength( out_node.params.orig_ts_len);
			  
			  return out_node;
		  }
		  
		  //System.out.println( "SerDe NodeType: " + out_node.getType() ) ;

		  
			
		  return null;
		  
	  }
	
	
	
	
	
}

package tv.floe.lumberyard.hbase.isax.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.index.AbstractNode;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.NodeType;
import edu.hawaii.jmotif.datatype.isax.index.SerDeUtils;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;
import edu.hawaii.jmotif.datatype.isax.index.kNNSearchResults;

//import edu.hawaii.jmotif.datatype.isax.index.AbstractNode;

/**
 * 
 * Terminal node which lives in HBase
 * 
 * SerDe
 * 
 * -	
 * 
 * 
 */
public class TerminalNodePersisted extends NodePersisted {

	
	public HashMap<String, TimeseriesInstance> arInstances  = new HashMap<String, TimeseriesInstance>();

	  /**
	   * Constructor.
	   */
	  public TerminalNodePersisted( Sequence seq_key, IndexHashParams params ) {
	    super();
	    this.setType(NodeType.TERMINAL);
	    this.key = seq_key;
	    this.params = params;
	  }

	  /**
	   * CTOR - for use with HBase deserialization
	   * @param seq_key
	   */
	  public TerminalNodePersisted( Sequence seq_key ) {
		    super();
		    this.setType(NodeType.TERMINAL);
		    this.key = seq_key;

	  }	  

	  @Override
	  public boolean IsOverThreshold() {
		  
		  if ( this.params.threshold < 1 ) {
			  System.out.println( "bad threshold!" );
			  return false;
		  }
		  
		  if ( this.arInstances.size() > this.params.threshold ) {
			  //System.out.println( "threshold says split!" );
			  return true;
		  }
		  
		  return false;
		  
	  }

	  public Iterator getNodeInstancesIterator() {
		  return this.arInstances.keySet().iterator();
	  }


	  
	  public void DebugInstances() {
		  
		  System.out.println( "TerminalNode > DebugInstances ----- " );
		  
		  Iterator itr = this.arInstances.keySet().iterator();
		  
		    while(itr.hasNext()) {
		    	
		    	String strKey = itr.next().toString();
		    	
		    	System.out.println( "T-node-ts-key: " + strKey );
		    	this.arInstances.get(strKey).Debug();
		    	
				  if ( null == this.arInstances.get( strKey ) ) {
					  
					  System.out.println( "TerminalNode > Debug > Null: " + strKey + ", count: " + this.arInstances.size()  );
					  
				  }	else {
					  
				  }
		    	//new_node.Insert( node.arInstances.get(strKey) );
		    	
		    }
		    
		    System.out.println( "---------------------------------" );
							  
		  
		  
	  }

	  @Override
	  public void Insert( TimeseriesInstance ts_inst ) {
		  
			Sequence ts_isax = null;
			
			try {
				// lets get our SAX word based on the params of this node and its key
				ts_isax = ISAXUtils.CreateiSAXSequenceBasedOnCardinality( ts_inst.getTS(), this.key );
			} catch (TSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		  
		  
		  String isax_hash = ts_isax.getIndexHash(); //this.params.createMaskedBitSequence(isax);
		  	  
		   // termination check, AKA "is this me?"
		  
		  if ( this.key.getIndexHash().equals(isax_hash) ) {
			  
			  if ( this.arInstances.containsKey(ts_inst.getTS().toString() ) ) {
				  
				  // merge
				  
				  TimeseriesInstance ts_int_existing = this.arInstances.get( ts_inst.getTS().toString() );
				  ts_int_existing.AddOccurences(ts_inst);
				  
			  } else {
				  
				  
				  
				  if ( null == ts_inst ) {
					  System.out.println( "Terminal Node > ************************ null add!" );
				  }
				  
				  
				  // add
				  try {
					  
					//  System.out.println( "add > key > " + ts_inst.getTS().toString() );
					  
					this.arInstances.put( ts_inst.getTS().toString(), ts_inst.clone() );
				} catch (CloneNotSupportedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				  
			  }
			  
			  if ( this.params.bDebug ) {
				  System.out.println( "|" );
			  }
			  
			//  this.DebugInstances();
			  
			//  System.out.println( "TerminalNode > Inserted(instances:" + this.arInstances.size() + ") > " + isax_hash + " @ " + this.key.getStringRepresentation() + ", occurrences: " + this.arInstances.get( ts_inst.getTS().toString() ).getOccurences().size() ) ;
			  
			  
			  // ------ ok, either way at this point, the data structures are updated. ---- update hbase -----
			  
			  
			  byte[] b = this.getBytes();
			  
				try {
					
					HBaseUtils.Put( this.key.getIndexHash(), this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, b );
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
		//		System.out.println( "\n\nTerminalNodePersisted > Structures Updated > " + b.length + " bytes Written to HBase!" );
			  
		  } else {
			  
			  // ok, how did we get here?
			  
			  System.out.println( "Should not have recv'd a ts at this TerminalNode!!!" );
			  
		  }

		  
		  
		  
	  }

	  
	  
	  
	  @Override
	  public TimeseriesInstance ApproxSearch( Timeseries ts ) {
		  

			Sequence ts_isax = null;
			  
			  AbstractNode node = null;
		 
				
				//ArrayList<Integer> arCards = IndexHashParams.generateChildCardinality( this.key );
			  ArrayList<Integer> arCards = this.key.getCardinalities();
							  
				Sequence seq_out_0;
				
				try {
					ts_isax = ISAXUtils.CreateiSAXSequenceBasedOnCardinality( ts, arCards );
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  		  
		  
				System.out.println( "Terminal Node Debug > Approx Search " );
				//System.out.println( "Searching For > Seq: " + ts_isax.getStringRepresentation() );
				System.out.println( "Searching For > Seq: " + ts_isax.getIndexHash() );
				System.out.println( "Searching For > ts: " + ts.toString() );
				this.DebugInstances();
				  
				  String isax_hash = ts_isax.getIndexHash(); //this.params.createMaskedBitSequence(isax);
				  	  
				   // termination check, AKA "is this me?"
				  
				  System.out.println( "key( " + this.key.getIndexHash() + " ) == search( " + isax_hash + " ) ?" );
				  
				  if ( this.key.getIndexHash().equals(isax_hash) ) {
					  
					  System.out.println( "found > key > " + isax_hash + ", looking for exact match" );

					  if ( this.arInstances.containsKey(ts.toString() ) ) {
						  
						  System.out.println( "found match!" );
						  
						  return this.arInstances.get(ts.toString() );
						  
					  }
					  
				  } // if
				  
				  
			return null;	  

	  }  

	  

/*	  @Override
	  public void LoadFromStore( String sax_hash_key ) {
		  
		  this.key = new Sequence( 0 );
		  this.key.parseFromIndexHash( sax_hash_key );
		  
		  // CALL to hbase here, pull bytes @ key: sax_hash_key
		  
		  byte[] node_bytes = null; // TODO ---------- wire in HBASE
		  
		  // deserialize bytes into Node
		  this.deserialize( node_bytes );
		  
		  // add orig length into sax key
		  this.key.setOrigLength( this.params.orig_ts_len );
	  }

	  @Override
		public void WriteToStore() {
			
			
			
			// hash key to get HBase key
		  String key = this.key.getIndexHash();
		  
		  // serialize node into bytes
		  byte[] node_bytes = this.getBytes();
		  
		  // PUT into hbase
			
		  // TODO ---------- wire in HBASE
			
		}	  
*/	  
	  
	  
	  
	  public void  deserialize(byte[] src_bytes) {
	  
		  int nt_d = SerDeUtils.byteArrayToInt(src_bytes, 0);
		  
		  if ( 0 == nt_d ) {
			  System.out.println( "TerminalNodePersisted > deserialize > ERR > SerDe reports a terminal node as ROOT" );
			  this.setType(NodeType.ROOT);
		  } else if (1 == nt_d) {
			  System.out.println( "TerminalNodePersisted > deserialize > ERR > SerDe reports a terminal node as INTERNAL" );
			  this.setType(NodeType.INTERNAL);
		  } else if (2 == nt_d) {
			  this.setType(NodeType.TERMINAL);
		  }
		  
		//  System.out.println( "nt: " + this.getType() ) ;
		  
		  
		  this.params = new IndexHashParams();
		  this.params.deserialize(src_bytes, 4);
		  
		  byte[] instance_hm_bytes = new byte[ src_bytes.length - 28 ]; 
			  
		  System.arraycopy(src_bytes, 28, instance_hm_bytes, 0, src_bytes.length - 28);

			ObjectInputStream in = null;
			try {
				in = new ObjectInputStream( new ByteArrayInputStream( instance_hm_bytes ));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//HashMap<String, TimeseriesInstance> o = null;
			
			try {
				this.arInstances = (HashMap<String, TimeseriesInstance>) in.readObject();
			
				in.close();
				
			//	System.out.println( "TerminalNodePersisted > SerDe > TerminalNodePersisted > num instances : " + this.arInstances.size() );
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		  
		  
	  }
	  
		@Override
		public byte[] getBytes() {
			
/*			
 *  		-	InternalNode
 *  
 *  			-	Node Type (4)
 *  			-	IndexHashParams (24)
 *  			-	descendants (array of key strings) (?)
 */			
	
			//byte[] out = new byte[]
			
			//int keys_size = -1;
	//		Set<String> keys = null; // this.descendants.keySet();
	//		ArrayList<String> arKeys = new ArrayList<String>();
	//		arKeys.addAll(keys);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream out = null;
			try {
				out = new ObjectOutputStream( baos );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		//	System.out.println( "bytes: " + baos.size() );
			
			try {
				out.writeObject( this.arInstances );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//System.out.println( "bytes: " + baos.size() );
			byte[] hashmap_bytes = baos.toByteArray();
			
			byte[] out_bytes = new byte[ 28 + hashmap_bytes.length ];
			//System.arraycopy(src, srcPos, dest, destPos, length)
			
			int nt = -1;
			if ( this.getType() == NodeType.ROOT ) {
				nt = 0;
			} else if ( this.getType() == NodeType.INTERNAL ) {
				nt = 1;
			} else if ( this.getType() == NodeType.TERMINAL ) {
				nt = 2;
			}
			
			//System.arraycopy(, srcPos, dest, destPos, length)
			
			SerDeUtils.writeIntIntoByteArray( nt, out_bytes, 0 );
			
			byte[] index_params_bytes = null;
			
			try {
				index_params_bytes = this.params.getBytes();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			
			}
			
			System.arraycopy(index_params_bytes, 0, out_bytes, 4, index_params_bytes.length);
			
			System.arraycopy(hashmap_bytes, 0, out_bytes, 4 + index_params_bytes.length, hashmap_bytes.length);
			
			return out_bytes;
			
		}

	  @Override
	  public TimeseriesInstance getNodeInstanceByKey( String strKey ) {
		  return this.arInstances.get(strKey);
	  }
	  
	  @Override
	  public HashMap<String, TimeseriesInstance> getNodeInstances() {
		  return this.arInstances;
	  }
	  	
	
}

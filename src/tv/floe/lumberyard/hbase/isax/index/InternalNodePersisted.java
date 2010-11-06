package tv.floe.lumberyard.hbase.isax.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.index.AbstractNode;
import edu.hawaii.jmotif.datatype.isax.index.HashTreeException;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.InternalNode;
import edu.hawaii.jmotif.datatype.isax.index.NodeType;
import edu.hawaii.jmotif.datatype.isax.index.SerDeUtils;
import edu.hawaii.jmotif.datatype.isax.index.TerminalNode;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;


/**
 * 
 * Internal Node for HBase isax index
 * 
 * ToDo
 * 
 * -	what triggers a deserialization?
 * 		-	object creation with key
 * 
 * 
 * - we only load the keys from hbase, we deserialize child nodes into slots as needed
 * 
 * 
 * @author jpatterson
 *
 */
public class InternalNodePersisted extends NodePersisted {

	private TreeMap<String, AbstractNode> descendants = new TreeMap<String, AbstractNode>();
	
	  	  
	  /**
	   * 
	   * 
	   * @param isax_base_rep
	   * @param params
	   * @param nt
	   */
	  public InternalNodePersisted( Sequence isax_base_rep, IndexHashParams params, NodeType nt ) {
		
		  this.params = params;
		  this.setType(nt);
		  this.key = isax_base_rep;
		  
		  
	  }
	  
	  public InternalNodePersisted() {
		  
		  
	  }

/*	  @Override
	  public void LoadFromStore( String sax_hash_key ) {
		  
		  this.key = new Sequence( 0 );
		  this.key.parseFromIndexHash( sax_hash_key );
		  
		  // CALL to hbase here, pull bytes @ key: sax_hash_key

		  //NodePersisted hbase_node = null;
			byte[] node_bytes = null;
			try {
				node_bytes = HBaseUtils.Get( sax_hash_key, "isax_index_test", "node", "store");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			
			//hbase_node = NodePersisted.deserialize_unknown(node_bytes);
			//this.deserialize(node_bytes);
			//hbase_node.key = new Sequence( 0 );
			//hbase_node.key.parseFromIndexHash( isax_hash );
			//hbase_node.key.setOrigLength( hbase_node.params.orig_ts_len );
		  
		  
		  
		  //byte[] node_bytes = null;
		  
		  // deserialize bytes into Node
		  this.deserialize( node_bytes );
		  
		  // add orig length into sax key
		  this.key.setOrigLength( this.params.orig_ts_len );
	  }
*/
/*	  @Override
		public void WriteToStore() {
			
			
			
			// hash key to get HBase key
		  String key = this.key.getIndexHash();
		  
		  // serialize node into bytes
		  byte[] node_bytes = this.getBytes();
		  
		  // PUT into hbase
			
			
		}
*/	  
	  public void debug_helper() {
		  
		  this.descendants.put("a", null);
		  this.descendants.put("z", null);
		  this.descendants.put("1", null);
		  this.descendants.put("10", null);
		  
	  }
	  
	  public void  deserialize(byte[] src_bytes) {
	  
		  int nt_d = SerDeUtils.byteArrayToInt(src_bytes, 0);
		  
		  if ( 0 == nt_d ) {
			  this.setType(NodeType.ROOT);
		  } else if (1 == nt_d) {
			  this.setType(NodeType.INTERNAL);
		  } else if (2 == nt_d) {
			  this.setType(NodeType.TERMINAL);
		  }
		  
		 // System.out.println( "nt: " + this.getType() ) ;
		  
		  
		  this.params = new IndexHashParams();
		  this.params.deserialize(src_bytes, 4);
		  
		  byte[] keys_bytes = new byte[ src_bytes.length - 28 ]; 
			  
		System.arraycopy(src_bytes, 28, keys_bytes, 0, src_bytes.length - 28);

			ObjectInputStream in = null;
			try {
				in = new ObjectInputStream( new ByteArrayInputStream( keys_bytes ));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			ArrayList<String> o = null;
			
			try {
				o = (ArrayList<String>) in.readObject();
			
				in.close();
				
				for ( int x = 0; x < o.size(); x++ ) {
					
				//	System.out.println( x + " > " + o.get(x) );
					this.descendants.put(o.get(x), new InternalNode());
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		  
		  
	  }
	  
	  /**
	   * We dont serizlize the key because it comes from the parent set of keys,
	   *  BUT ---- we have to remember to pull the orig ts length from index hash params
	   */
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
			Set<String> keys = this.descendants.keySet();
			ArrayList<String> arKeys = new ArrayList<String>();
			arKeys.addAll(keys);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream out = null;
			try {
				out = new ObjectOutputStream( baos );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//System.out.println( "bytes: " + baos.size() );
			
			try {
				out.writeObject(arKeys);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//System.out.println( "bytes: " + baos.size() );
			byte[] keys_bytes = baos.toByteArray();
			
			byte[] out_bytes = new byte[ 28 + keys_bytes.length ];
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
			
			System.arraycopy(keys_bytes, 0, out_bytes, 4 + index_params_bytes.length, keys_bytes.length);
			
			return out_bytes;
			
		}
	  
	  
	/**
	 * 
	   * When a split occurs in SAX space, we turn a terminal node into an internal node, which then changes how it 
	   * handles inserts: example: internal node A now points to B and C
	   * 
	   * node A's SAX key stays the same, yet B and C have a split in SAX space based on increasing the dim 
	   * on the lowest cardinality in the array of symbols: 0^2 becomes 00^4 and 01^4, where 1^2 becomes 10^4 and 11^4
	   * 
	   * when we insert a new ts in this internal node, we hash at the card of its key (params)
	   * 
	   * if this new hash rep of the ts is in the hash-table, we pass it on to that node for insertion
	 * 
	 *    Need to delineate logic wise about when to check the node itself vs its decendants.
	 * @throws HashTreeException 
	 */
	  @Override
	  public void Insert( TimeseriesInstance ts_inst ) throws HashTreeException {

	//	  System.out.println( "InternalNodePersisted > Insert > " );
		//  this.DebugKeys();
		  
			Sequence ts_isax = null;
			
			if ( null == ts_inst ) {
				
				System.out.println( "ts_inst came in null!!" );
				
				throw new HashTreeException("null ts!");
			}
	  
			
		  // we know this is not the final resting place for this ts since this is an InternalNode
		  
		  
		  //AbstractNode node = null;
		  
		  if ( this.getType() == NodeType.ROOT ) {

				
				try {
					// lets get our SAX word based on the params of this node and its key
					ts_isax = ISAXUtils.CreateiSAXSequence( ts_inst.getTS(), this.params.base_card, this.params.isax_word_length);
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
		  } else {

			  
				
				ArrayList<Integer> arCards = IndexHashParams.generateChildCardinality( this.key );
							  
				//Sequence seq_out_0;
				
				try {
					
					if ( ts_inst == null ) {
						System.out.println( "ts_inst nullll" );
					}
					
					if ( null == ts_inst.getTS() ) {
						System.out.println( "getTS() null" );
					}
					
					if ( null == arCards ) {
						System.out.println( "arCards null" );
					}
					
					//System.out.println( "Internal Node > Cards > " + arCards );
					if ( this.params.bDebug ) {
						System.out.print( "." );
					}
					
					ts_isax = ISAXUtils.CreateiSAXSequenceBasedOnCardinality( ts_inst.getTS(), arCards );
					
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  		  

			  
		  }
		  
		  if ( ts_inst == null ) {
			  
			  System.out.println( "bad inst!" );
			  
		  }
		  
		  if ( null == ts_isax ) {
			  // failed to insert
			  System.out.println( " InternalNode > Insert > Fail > " + ts_inst.getTS() );
			  return;
		  }
		  
			  
			  String isax_hash = ts_isax.getIndexHash(); //this.params.createMaskedBitSequence(isax);
					  
			  /*
			   * 
			   * HBase Storage notes
			   * 
			   * -	at this point, we should have deserialized the decendants?
			   * 		-	the keys are loaded
			   * 
			   */
			  // we want to fan out at a rate of 2^d
			  
			  if ( this.descendants.containsKey(isax_hash) ) {
				  
				 // System.out.println( "InternalNodePersisted > The Hash Key Exists!!! NOT IMPL!!!" );
				  
				  //
				  
			//	  System.out.println( "\n\nInternalNodePersisted > key: " + isax_hash + " was found in this node's (" + this.key.getIndexHash() + ") descendants"  );
				  if ( this.getType() == NodeType.ROOT ){
			//		  System.out.println( "----> ROOT\n " );
				  }
				  

				  /*
				  NodePersisted hbase_node = null;
					byte[] node_bytes = null;
					try {
						node_bytes = HBaseUtils.Get( isax_hash, "isax_index_test", ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}		
					
					hbase_node = NodePersisted.deserialize_unknown(isax_hash, node_bytes);
*/
				   NodePersisted hbase_node = NodePersisted.LoadFromStore( isax_hash, this.GetTableName() );
				  
					
					
				  if ( hbase_node.getType() == NodeType.TERMINAL ) {
					  
					  
					  if ( hbase_node.IsOverThreshold() == false ) {

						  
						  if ( null == ts_inst ) {
							  System.out.println( "Internal Node > pre split!" );
						  }
						  
									
					//	  System.out.println( ts_inst +  " -> " + isax_hash );
						  
						  hbase_node.Insert( ts_inst ); // should be terminal node
						  
					  } else {

						  // need to change this to "InternalNode.cloneAs( NodeType )"
						  InternalNodePersisted new_internal_hbase_node = new InternalNodePersisted( hbase_node.key, hbase_node.params, NodeType.INTERNAL );
						  new_internal_hbase_node.SetTableName(this.GetTableName());
						  
						  new_internal_hbase_node.Insert( ts_inst );
						  
							//Iterator itr = node.getChildNodeIterator();
						  Iterator itr = hbase_node.getNodeInstancesIterator();
							
						    while(itr.hasNext()) {
						    	
						    	String strKey = itr.next().toString();
						    	
						    	
						    	//new_node.Insert( node.arInstances.get(strKey) );
						    	new_internal_hbase_node.Insert( hbase_node.getNodeInstanceByKey(strKey) );
						    	
						    }
						    
						    //new_node.DebugKeys();
												  
						  // lets update the local cache
						  this.descendants.remove(isax_hash);
						  
						  // now remove the old Terminal Rep from hbase
						  
							try {
								HBaseUtils.DeleteRow(isax_hash, this.GetTableName());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}								  
						  
						  // update the current node's local pointer cache
						  this.descendants.put(isax_hash, new_internal_hbase_node);

						  // how lets update this representation in hbase
							try {
								HBaseUtils.Put( this.key.getIndexHash(), this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, this.getBytes() );
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}						  
						  
							// then lets insert the new internal node into hbase
						  
							try {
								HBaseUtils.Put( new_internal_hbase_node.key.getIndexHash(), this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, new_internal_hbase_node.getBytes() );
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}								
							
					  }
					  
					  
				  } else if ( hbase_node.getType() == NodeType.INTERNAL ) {
/*					  
					  if ( ts_inst == null ) {
						  System.out.println( "Insert of null! " + ts_inst.toString() );
					  }
	*/				  
					  hbase_node.Insert( ts_inst );
					  
				  }
			  
			  } else {
				  
				  // if it does not contain this node, create a new one
				  
				  // create a key seqeunce based on the base cardinality
				  TerminalNodePersisted hbase_node = new TerminalNodePersisted( ts_isax, this.params );
				  hbase_node.SetTableName(this.GetTableName());
				  
			//	  System.out.println( "inserting new terminal node: " + isax_hash );
				  
				  hbase_node.Insert( ts_inst );
				  /*
				   * We have to also update the parent's descendants
				   * 
				   */
				  
				  this.descendants.put(isax_hash, hbase_node); // node is not actually serialized into hbase, just keys
					try {
						
						String this_key = this.key.getIndexHash();
						
						if (this.getType() == NodeType.ROOT) {
							this_key = ISAXIndex.ROOT_NODE_KEY;
						}
						
						HBaseUtils.Put( this_key, this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, this.getBytes() );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}				  
					
					byte[] bytes_node = hbase_node.getBytes();
					
					try {
						HBaseUtils.Put( isax_hash, this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME, bytes_node );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				  
				  
			  }
		  
	  } 
	  
	  public void DebugKeys() {
		  
		  Iterator i = this.descendants.keySet().iterator();
		  
		  if ( this.getType() == NodeType.ROOT ) {
			  System.out.println( "Debug > Root Node" );
		  }
		  
		  System.out.println( "DebugKeys > " + this.key.getBitStringRepresentation() );
		  while (i.hasNext()) {
			  
			  String key = (String) i.next();
			  
			  System.out.println( "Node Key > Debug > " + key );
			  
		  }
	  }
	  
	  
	  
	  public void DebugChildNodes() {
		  
		  Iterator i = this.descendants.keySet().iterator();
		  
		  if ( this.getType() == NodeType.ROOT ) {
			  System.out.println( "Debug > Root Node" );
		  }
		  
		  System.out.println( "Debug > This Node's Key > " + this.key.getBitStringRepresentation() );
		  while (i.hasNext()) {
			  
			  String key = (String) i.next();
			  
			  System.out.println( "Decendant Node Key > Debug > " + key + ", instances: " + this.descendants.get(key).getNodeInstances().size() );
			  
			  
			  
		  }
	  } 
	  
	  
	  @Override
	  public TimeseriesInstance ApproxSearch( Timeseries ts ) {
		  

			Sequence ts_isax = null;
			  
			  AbstractNode node = null;
		  
		  
		  if ( this.getType() == NodeType.ROOT ) {

				
				try {
					// lets get our SAX word based on the params of this node and its key
					//ts_isax = ISAXUtils.CreateiSAXSequenceBasedOnCardinality( ts, this.key );
					ts_isax = ISAXUtils.CreateiSAXSequence( ts, this.params.base_card, this.params.isax_word_length);
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
				//System.out.println( "Debug > Root Node!" + ts_isax.getStringRepresentation() );
				
		  } else {

			  
				
				ArrayList<Integer> arCards = IndexHashParams.generateChildCardinality( this.key );
							  
				Sequence seq_out_0;
				
				try {
					ts_isax = ISAXUtils.CreateiSAXSequenceBasedOnCardinality( ts, arCards );
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			//	System.out.println( "Debug > Internal Node!" );
			  		  

			  
		  }
		  
			  
			  String isax_hash = ts_isax.getIndexHash(); //this.params.createMaskedBitSequence(isax);
					  
			  
				  // we want to fan out at a rate of 2^d
			  
			  if ( this.descendants.containsKey(isax_hash) ) {
				  
				  
/*				  
				  NodePersisted hbase_node = null;
					byte[] node_bytes = null;
					try {
						node_bytes = HBaseUtils.Get( isax_hash, this.GetTableName(), ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}		
					
					hbase_node = NodePersisted.deserialize_unknown( isax_hash, node_bytes );
*/				  
				   NodePersisted hbase_node = NodePersisted.LoadFromStore( isax_hash, this.GetTableName() );
			  
				  
				  
				  
				  
				  return hbase_node.ApproxSearch(ts);
				  
				  
			  } else {
				  
				  // if it does not contain this node

				  System.out.println( "InternalNodePersisted > ERROR > Debug > no descendant contained a key for this level!" );
				  return null;

				  
			  }	  
	  }
	
	
}

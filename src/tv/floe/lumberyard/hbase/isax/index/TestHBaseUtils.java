/**
   Copyright [2011] [Josh Patterson]

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

 */

package tv.floe.lumberyard.hbase.isax.index;

import java.io.IOException;
import java.util.HashMap;

import edu.hawaii.jmotif.datatype.TPoint;
import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;

public class TestHBaseUtils {

	
	public static void testNodeSerDe_PUT() throws IOException {
		
		
		Timeseries ts_1 = new Timeseries();
		
		ts_1.add( new TPoint( 1.0, 0) );
		ts_1.add( new TPoint(-0.5, 1) );
		ts_1.add( new TPoint(-0.25, 2) );
		ts_1.add( new TPoint( 0.0, 3) );
	
		ts_1.add( new TPoint( 0.25, 4) );
		ts_1.add( new TPoint( 0.50, 5) );
		ts_1.add( new TPoint( 0.75, 6) );
		ts_1.add( new TPoint( -2.0, 7) );		  		

		
		Timeseries ts_2 = new Timeseries();
		
		ts_2.add( new TPoint(1.0, 0) );
		ts_2.add( new TPoint(-0.5, 1) );
		ts_2.add( new TPoint(-0.25, 2) );
		ts_2.add( new TPoint( 0.0, 3) );
	
		ts_2.add( new TPoint( 0.25, 4) );
		ts_2.add( new TPoint( 0.50, 5) );
		ts_2.add( new TPoint( 0.75, 6) );
		ts_2.add( new TPoint( -2.1, 7) );		  

		
		
	
		
		Timeseries ts_3 = new Timeseries();
		
		ts_3.add( new TPoint(1.0, 0) );
		ts_3.add( new TPoint(-0.5, 1) );
		ts_3.add( new TPoint(-0.25, 2) );
		ts_3.add( new TPoint( 0.0, 3) );
	
		ts_3.add( new TPoint( 0.25, 4) );
		ts_3.add( new TPoint( 0.50, 5) );
		ts_3.add( new TPoint( 0.75, 6) );
		ts_3.add( new TPoint( -1.9, 7) );				
		
		
		Sequence seq = null;
		try {
			seq = ISAXUtils.CreateiSAXSequence(ts_2, 4, 4);
		} catch (TSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//IndexHashParams params = new IndexHashParams();
		IndexHashParams p = new IndexHashParams();
		  p.base_card = 4;
		  p.d = 1;
		  p.isax_word_length = 4;
		  p.orig_ts_len = 8;
		  p.threshold = 2;
		  
		
		TerminalNodePersisted node = new TerminalNodePersisted(seq, p);
	
		
		TimeseriesInstance tsi_A = new TimeseriesInstance( ts_1 );
		tsi_A.AddOccurence("foo.txt", 10 );

		
		TimeseriesInstance tsi_B = new TimeseriesInstance( ts_2 );
		tsi_B.AddOccurence("foo.txt", 1);

		TimeseriesInstance tsi_C = new TimeseriesInstance( ts_3 );
		tsi_B.AddOccurence("foo.txt", 12);
		
		
		node.Insert( tsi_A );
		node.Insert( tsi_B );
		node.Insert( tsi_C );
		
		System.out.println( " size: " + node.arInstances.size() );		
		
		byte[] bytes_node = node.getBytes();
		
		HBaseUtils.Put( seq.getIndexHash(), "isax_index_test", "node", "store", bytes_node );
		
		System.out.println( "\n\nHBase.PUT -> " + seq.getIndexHash() );
		
		
	}
	
	public static void testNodeSerDe_GET() throws IOException {
		
		
		Timeseries ts_2 = new Timeseries();
		
		ts_2.add( new TPoint(1.0, 0) );
		ts_2.add( new TPoint(-0.5, 1) );
		ts_2.add( new TPoint(-0.25, 2) );
		ts_2.add( new TPoint( 0.0, 3) );
	
		ts_2.add( new TPoint( 0.25, 4) );
		ts_2.add( new TPoint( 0.50, 5) );
		ts_2.add( new TPoint( 0.75, 6) );
		ts_2.add( new TPoint( -2.1, 7) );		  

		
		
		
		Sequence seq = null;
		try {
			seq = ISAXUtils.CreateiSAXSequence(ts_2, 4, 4);
		} catch (TSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

		byte[] node_bytes = HBaseUtils.Get(seq.getIndexHash(), "isax_index_test", "node", "store");
		
		TerminalNodePersisted serde_node = new TerminalNodePersisted(seq);
		
		serde_node.deserialize(node_bytes);
		
		serde_node.key.setOrigLength(serde_node.params.orig_ts_len);
		
		serde_node.DebugInstances();
		
		TimeseriesInstance ts_answer = serde_node.getNodeInstanceByKey(ts_2.toString());
		
		HashMap<String, Long> l1 = ts_answer.getOccurences();
		
		System.out.println( "occurences: " + l1.size() );
				
		
	}
	
	
	public static void main(String [ ] args)
	{
	
		try {
			//HBaseUtils.Put("row_test", "isax_index_test", "node", "store", Bytes.toBytes("test from java: " + System.currentTimeMillis()));
			//HBaseUtils.testNodeSerDe_PUT();
			
			TestHBaseUtils.testNodeSerDe_GET();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	
	}
	
	
}

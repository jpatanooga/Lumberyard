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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Set;

import org.junit.Test;

import edu.hawaii.jmotif.datatype.TPoint;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.Sequence;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.NodeType;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;

public class TestInternalNodePersisted {

	@Test
	public void testSerDe() {

		
		  IndexHashParams p = new IndexHashParams();
		  p.base_card = 4;
		  p.d = 1;
		  p.isax_word_length = 4;
		  p.orig_ts_len = 8;
		  p.threshold = 100;

		  Sequence s = new Sequence( 8 ); // root node seqeunce, needs nothing more than a word len
		  		
		
		InternalNodePersisted n = new InternalNodePersisted( s, p, NodeType.INTERNAL );
		n.debug_helper();
		
		byte[] rep = n.getBytes();
		
		System.out.println( "internal bytes: " + rep.length );
		
		InternalNodePersisted n_d = new InternalNodePersisted();
		n_d.deserialize(rep);

		n_d.key = new Sequence( 0 );
		n_d.key.parseFromIndexHash( "1.1" );

		n_d.key.setOrigLength(n_d.params.orig_ts_len);
		
		System.out.println( "params: " + n_d.params.threshold + ", o-len: " + n_d.params.orig_ts_len );
//		System.out.println( "children: " +  );
		n_d.DebugKeys();
		
	}
	
	@Test
	public void testComplexObjSerDe() {
		
		System.out.println("> testComplexObjSerDe ----------------- "  ); 
		
		Timeseries ts = new Timeseries();
		
		ts.add( new TPoint(-1.0, 0) );
		ts.add( new TPoint(-0.5, 1) );
		ts.add( new TPoint(-0.25, 2) );
		ts.add( new TPoint( 0.0, 3) );
	
		ts.add( new TPoint( 0.25, 4) );
		ts.add( new TPoint( 0.50, 5) );
		ts.add( new TPoint( 0.75, 6) );
		
		// the one we change
		ts.add( new TPoint( 1.0, 7) );

		
		//Set<String> keys = this.descendants.keySet();
		ArrayList<TimeseriesInstance> arKeys = new ArrayList<TimeseriesInstance>();
		
		TimeseriesInstance tsi = new TimeseriesInstance(ts);
		tsi.AddOccurence("a", 10);
		tsi.AddOccurence("b", 20);
		tsi.AddOccurence("c", 30);
		
		arKeys.add( tsi );
		arKeys.add( new TimeseriesInstance(ts));
		arKeys.add( new TimeseriesInstance(ts));
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream( baos );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println( "bytes: " + baos.size() );
		
		try {
			out.writeObject(arKeys);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println( "complex obj bytes: " + baos.size() );
		//byte[] keys_bytes = baos.toByteArray();
		
		//byte[] out_bytes = new byte[ 28 + keys_bytes.length ];		

		
		
		

		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream( new ByteArrayInputStream( baos.toByteArray() ));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ArrayList<TimeseriesInstance> o = (ArrayList<TimeseriesInstance>) in.readObject();
		
			in.close();
			
			for ( int x = 0; x < o.size(); x++ ) {
				
				System.out.println( x + " > " + o.get(x).getTS().toString() );
				
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        				
		
		
		
	}
	
	
	
}

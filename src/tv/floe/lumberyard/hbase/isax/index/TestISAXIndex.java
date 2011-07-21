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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import edu.hawaii.jmotif.datatype.TPoint;
import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.index.IndexHashParams;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;
//import edu.hawaii.jmotif.datatype.isax.index.iSAXIndex;


/**
 * 
 * These are some example use cases with Lumberyard
 * 
 * Two primary ways we want to cover
 * 
 * 	-	using Lumberyard as a simple java client 
 * 		-	query a specific table for a pattern
 * 		-	may have multiple tables in same hbase, all use similar schema though
 * 		-	table specified in query call
 * 		-	search-pattern becomes key
 * 
 * 	-	using Lumberyard via a http-service like Tornado
 * 		-	would want to specify the table from the REST query
 * 		-	would want to specify the search-pattern from the REST query -> search-query becomes key
 * 		-	schema is hard coded?
 * 
 * 	-	TODO
 * 		-	think about loading schemes and how tables get created.
 * 
 * @author jpatterson
 *
 */
public class TestISAXIndex {



	public static void insert_test() {
		

		ISAXIndex index = new ISAXIndex();
		
		System.out.println( "\ntestISAXIndex_1" );
		
		//iSAXIndex index = new iSAXIndex( 4, 4, 8 );
	
		
		Timeseries ts = new Timeseries();
		
		ts.add( new TPoint(-1.0, 0) );
		ts.add( new TPoint(-0.5, 1) );
		ts.add( new TPoint(-0.25, 2) );
		ts.add( new TPoint( 0.0, 3) );
	
		ts.add( new TPoint( 0.25, 4) );
		ts.add( new TPoint( 0.50, 5) );
		ts.add( new TPoint( 0.75, 6) );
		ts.add( new TPoint( 1.0, 7) );
		
		TimeseriesInstance tsi_A = new TimeseriesInstance( ts );

		
		for ( int x = 0; x < 3; x++ ) {
			index.InsertSequence(ts, "genome.txt", 204526 + x );
		}
		//index.InsertSequence(ts, "genome.txt", 104530 );		
		
	}
	
	
	public static void SearchTest() {
		
		System.out.println( "\nLumberyard > Search Test > Approximate Search" );

		ISAXIndex index = new ISAXIndex();
		
		Timeseries ts = new Timeseries();
		
		ts.add( new TPoint(-1.0, 0) );
		ts.add( new TPoint(-0.5, 1) );
		ts.add( new TPoint(-0.25, 2) );
		ts.add( new TPoint( 0.0, 3) );
	
		ts.add( new TPoint( 0.25, 4) );
		ts.add( new TPoint( 0.50, 5) );
		ts.add( new TPoint( 0.75, 6) );
		ts.add( new TPoint( 1.0, 7) );
		
		System.out.println( "Searching for: " + ts );
		
		//index.InsertSequence(ts_2, "genome.txt", 104526 );
		long start = System.currentTimeMillis();
		
		TimeseriesInstance result = index.ApproxSearch( ts );
		
		long diff = System.currentTimeMillis() - start;
		
		if (result == null) {
			System.out.println( "Approx Search > no result found!" );
		//	assertEquals( "null check", true, false );
		} else {
			System.out.println( "Found ts in " + diff + " ms" );
		}
		
		result.Debug();
		
	}
	
	public static void RandomInsertAndSearchTest( String table_name, int numberInserts ) {

		System.out.println( "\nLumberyard > Search Test > Random Insert Then Approximate Search" );

		ISAXIndex index = new ISAXIndex();
		
		if ( false == index.LoadIndex(table_name) ) {
			
			System.out.println( "\n\nCannot Load Index: " + table_name + ", quitting" );
			return;
			
		}
		
		int sampleSize = index.root_node.params.orig_ts_len;
		
		Timeseries search_ts = null;
		
		for ( int x = 0; x < numberInserts; x++ ) {
			System.out.print("." + x);
			Timeseries ts_insert = ISAXUtils.generateRandomTS( sampleSize );
			if ( x == (numberInserts / 2) ) {
				search_ts = ts_insert;
			}
			index.InsertSequence( ts_insert, "ts.txt", 1000 + x * 20);
		}
		
		System.out.println( " ----- insert done -------" );
		
		System.out.println( "Searching For: " + search_ts );
		
		long start = System.currentTimeMillis();
		
		TimeseriesInstance result = index.ApproxSearch( search_ts );
		
		long diff = System.currentTimeMillis() - start;
		
		if (result == null) {
			System.out.println( "Approx Search > no result found!" );
		//	assertEquals( "null check", true, false );
		} else {
			System.out.println( "Found ts in " + diff + " ms" );
		}
		
		result.Debug();		
		
		
	}
	

	
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		//RandomInsertAndSearchTest();
		//IndexGenomeFile( "", "" );
	
				
	}

}

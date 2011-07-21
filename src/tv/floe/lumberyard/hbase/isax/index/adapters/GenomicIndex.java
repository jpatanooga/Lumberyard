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

package tv.floe.lumberyard.hbase.isax.index.adapters;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import tv.floe.lumberyard.hbase.isax.index.ISAXIndex;
import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;

public class GenomicIndex {


	
	/**
	 * 
	 * Insert a smaller DNA strand
	 * 
	 * 
	 */
	  public static void InsertDNASample( String table_name, String dna_sample, int window_len, String source_name ) {
			 
		  String dna_window = "";
		  int curr_offset = 0;
		  int beginIndex = 0;
		  int endIndex = 0;
	
			ISAXIndex index = new ISAXIndex();
			
			if ( false == index.LoadIndex(table_name) ) {
				
				System.out.println( "\n\nCannot Load Index: " + table_name + ", quitting" );
				return;
				
			}
		  
		  
		  
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
							
				index.InsertSequence(ts_dna, source_name, beginIndex );
					
				curr_offset = endIndex;
				
				
				
			}	  
		  
	  }	
	
	
	public static void IndexGenomeFile( String table_name, String genome_file ) {
	
		
		ISAXIndex index = new ISAXIndex();
		
		if ( false == index.LoadIndex(table_name) ) {
			
			System.out.println( "\n\nCannot Load Index: " + table_name + ", quitting" );
			return;
			
		}

		int window_len = index.root_node.params.orig_ts_len;
		
		
		
		
		FileInputStream fis;
		InputStreamReader in = null;
		try {
			fis = new FileInputStream( genome_file );
			in = new InputStreamReader(fis, "UTF-8");
	
			char[] buf = new char[ window_len ];
			int bytesRead = 1;
			int offset = 0;
			
			Timeseries ts_dna = null;
			String sequence = "";
			
			while ( (bytesRead = in.read( buf, 0, window_len )) > 0 ) {
				
			//	bytesRead = in.read( buf, 0, 16 );
			
				sequence = String.copyValueOf(buf, 0, bytesRead );
				System.out.println( bytesRead + " >  " + sequence );
				
				
				
				// create ts
				try {
					ts_dna = ISAXUtils.CreateTimeseriesFromDNA( sequence );
				} catch (TSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				index.InsertSequence( ts_dna, genome_file, offset );
				
				
				offset += bytesRead;
			} 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
			if ( null != in ) {
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
			
		
	}	
	
	/**
	 * Returns a set of places (files and byte offsets) where the pattern occurs 
	 * 
	 * 
	 */
	public static GenomePatternOccurrences ApproxSearchForDNASequence( String index_name, String dna_sequence ) {
		
		// 1. Check / Load Index

		
		ISAXIndex index = new ISAXIndex();
		
		if ( false == index.LoadIndex( index_name ) ) {
			
			System.out.println( "\n\nCannot Load Index: " + index_name + ", quitting" );
			return null;
			
		}

		int window_len = index.root_node.params.orig_ts_len;
		
		Timeseries ts_dna = null;
		
		
		// 2. 
		
		// 3. change the dna sequence into a Timeseries
		
		// create ts
		try {
			ts_dna = ISAXUtils.CreateTimeseriesFromDNA( dna_sequence );
		} catch (TSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		TimeseriesInstance instances = index.ApproxSearch(ts_dna);
		
		GenomePatternOccurrences genomeOccur = null;
		
		if ( null == instances ) {
		
			
			
		} else {
			
			
			genomeOccur = new GenomePatternOccurrences( instances.getTS() );
			genomeOccur.AddOccurences(instances);

		}

		return genomeOccur;
		
	}
	

	
}

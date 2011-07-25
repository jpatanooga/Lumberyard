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

import tv.floe.lumberyard.hbase.isax.index.ISAXIndex;
import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;

public class TestGenomicIndex {

	
	
	public static void testDNASegmentInsertAndQuery( String table_name) {
		
		
		String human_dna = "GTCAATGGCCAGGATATTAGAACAGTACTCTGTGAACCCTATTTATGGTGGCACCCCTTAGACTAAGATAACACAGGGAGCAAGAGGTTGACAGGAAAGCCAGGGGAGCAGGGAAGCCTCCTGTAAAGAGAGAAGTGCTAAGTCTCCTTTCTAAGGCACATGATGGAT";
		human_dna += "TCAAGGGAAAGCCACATTTGACTAAAGCCCAAGGGATTGTTGCTTCTAATCCGATTTCTTGGCAGAAGATATTACAAACTAAGAGTCAGATTAATATGTGGGTGCCAAAATAAATAAACAAATAATTGAATAATCCCTGGAGGTTTAAGTGAGGAGAAACTCCTCCAC";
		human_dna += "AGCTTGCTACCGAGGCAGAACCGGTTGAAACTGAAATGCATCCGCCGCCAGAGGATCTGTAAAAGAGAGGTTGTTACGAAACTGGCAACTGCCAACCAAAGTCCACCAATGGACAAGCAAAAAAGAGCACTCATCTCATGCTCCCAAGGATCAACCTTCCCAGAGTTT";
		human_dna += "TCACTTAAGTGGCCACCAAGCCAGTTGTCAATCCAGGGCTTTGGACTGAAATCTAGGGCTTCATCCGCTACCTCAGAGTGTCTTCTATTTCTTCCAGCCAGTGACAAATACAACAAACATCTGAGATGTTTTAGCTATAAATCCTTTACAATTGTTATTTATGTCTTA";
		human_dna += "ACTTTTGTTATACCTGGAAAAGTAGGGGAAACAATAAGAACATACTGTCTTGGCCAAGCATCCAAGGTTAAATGAGTTATGGAAATTCATTTGGGAGCCAAGACATTGCACGTGGTTATTTATTAGTCACCCAAGCATGTATTTTGCATGTCCATCAGTTGTTCTTGG";
		human_dna += "CCAAAAGAGCAGAATCAATGAGCCGCTGCAGATGCAGACATAGCAGCCCCTTGCAGGGACAAGTCTGCAAGATGAGCATTGAAGAGGATGCACAAGCCCGGTAGCCCGGGAAATGGCAGGCACTTACAAGAGCCCAGGTTGTTGCCATGTTTGTTTTTGCAACTTGTC";
		human_dna += "TATTTAAAGAGATTTG";		

		
		String chimp_dna = "GGCAATGGCCAGGATATTAGAACAGTACTCTGTGAACCCTATTTATGGTAGCACCCCTTAGACTAAGATAACACAGGGAGCAAGAGGTTGACAGGAAAGCCAGGGGAGCAGGGAAGCCTCCTGTAAAGAGAGAAGTGCTAAGTCTCCTTTCTAAGGCACATGATGGAT";
		chimp_dna += "TCAAGGGAAAGTCACATTTGACTAAAGCCCAAGGGATTGTTGCTTCTAATCCGATTCTTGGCAGAAGATATTGCAAACTAAGAGTCAGATTAATATGTGGGTGCCAAAATAAATAAACAAATAATTGAATAATCCCTGGAGGTTTAAGTGAGGAGAAACTCCTCCACA";
		chimp_dna += "GCTTGCTACCGAGGCAGAACCGGTTGAAACTGAAATGCACCCGCTGCCAGAGGATCTGTAAAAGGGAGGTTGTTACCGAACTGGCAACTGCCAACCAAAGTCTACCAATGGACAAGCAAAAAAGAGCACTCATCTCATGCTCCCAAGGATCAACCTTCCCAGAATTTT";
		chimp_dna += "CACTTAAGTGGCCACCAAGCCAGTTGTCAATCCAGGGCTTTGGACTGAAATCTAGGGCTTCATCCACTACCTCAGAGTGTCTTCCATTTCTTCCAGCCAGTGACAAATACAACAAACATCTGAGATGTTTTAGCTATAAATCCTTTACAATTGTTATTTATGTCTTAA";
		chimp_dna += "CTTTTGTTATACCTGGAAAAGTAGGGGAAACAATAAGAACATACTGTCTTGGCCAAGCATCCAAGGTTAAATGAGTTATGGGAATTCATTTGGGAGCCAAGACATTGCGCGTGGTTATTTATTAGTCACCCAAGCATGTATTTTGCATGTCCATCAGTTGTTCTTGGC";
		chimp_dna += "CAAAAGAACAGAATCAATGAGCCGCTGCAGATGCAGACATAGCAGCCCCTTGCAGGAACAAGTCTGCAAGATGAGCATTGAAGAGGATGCACAAGCCCGGTAGCCCGGGAAATGGCAGGCACTTACAAGAGCCCAGGTTGTTGCCATGTTTGTTTTTGCAACTTGTCT";
		chimp_dna += "ATTTAAACAGATTTGA";		
		
		
		
		
		
				
		System.out.println( "\n\n------------------------\n\nDNA Index Test: sample size: " + human_dna.length() );
		

		ISAXIndex index = new ISAXIndex();
		
		if ( false == index.LoadIndex(table_name) ) {
			
			System.out.println( "\n\nCannot Load Index: " + table_name + ", quitting" );
			return;
			
		}
		
		int window_len = index.root_node.params.orig_ts_len;
		
		Timeseries search_ts = null;

		
//		ISAXIndex index = new ISAXIndex( 4, 4, window_len );
		
		System.out.println( "Indexing Human DNA Sample: \n" + human_dna );
		
			//index.InsertDNAString(human_dna, window_len, "human" );
		//GenomicIndex.IndexGenomeFile(table_name, human_dna);
		GenomicIndex.InsertDNASample(table_name, human_dna, window_len, "human");
			
		System.out.println( "Indexing Chimp DNA Sample: \n" + chimp_dna );			
			
		//	index.InsertDNAString(chimp_dna, window_len, "chimp" );
		//GenomicIndex.IndexGenomeFile(table_name, chimp_dna);
		GenomicIndex.InsertDNASample(table_name, chimp_dna, window_len, "chimp");
		
		
	}
	
	

/*	
	public static String DNAApproxSearch( String table_name, String sequence ) {

		Timeseries ts_dna = null;
		ISAXIndex index = new ISAXIndex();
		
		if ( false == index.LoadIndex(table_name) ) {
			
			System.out.println( "\n\nCannot Load Index: " + table_name + ", quitting" );

			return "";
		}
		
		try {
			ts_dna = ISAXUtils.CreateTimeseriesFromDNA( sequence );
		} catch (TSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		TimeseriesInstance result = index.ApproxSearch(ts_dna); 
		
		// reverse convert ts into DNA, return string
		
		return "";
		
	}	
*/	
	
}

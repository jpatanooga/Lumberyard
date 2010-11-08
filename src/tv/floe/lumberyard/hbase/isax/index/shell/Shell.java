package tv.floe.lumberyard.hbase.isax.index.shell;
/*
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
*/
import tv.floe.lumberyard.hbase.isax.index.HBaseUtils;
import tv.floe.lumberyard.hbase.isax.index.ISAXIndex;
//import tv.floe.lumberyard.hbase.isax.index.Shell;
import tv.floe.lumberyard.hbase.isax.index.TestISAXIndex;
import tv.floe.lumberyard.hbase.isax.index.adapters.GenomicIndex;
import tv.floe.lumberyard.hbase.isax.index.adapters.TestGenomicIndex;

public class Shell {

	
	
	public void printCreateIndexHelp() {
	
		System.out.println( "usage: Shell CreateIndex [table_name] <options>");
		System.out.println( "\tRequired Options:");
		System.out.println( "\t\t-base_card <card>");
		System.out.println( "\t\t-dim_split <dim_split>");
		System.out.println( "\t\t-base_word_len <len>");
		System.out.println( "\t\t-base_ts_sample_size <size>");
		System.out.println( "\t\t-split_threshold <th>");
		
		
		
	}
	
	public void CreateNewIndex( String[] args ) {
		
  	  //String table_name = argv[i++];

	  String strBaseCard = "4";
	  String strDimPerSplit = "1";
	  String strBaseWordLen = "4"; // has to be specified?
	  String strBaseTSSampleSize = "16"; // has to be specified
	  String strSplitThreshold = "100";
	  

	  if ( args.length != 12) {
		  
		  // no table name
		  printCreateIndexHelp();
		  return;
		  
	  } 
	  
	  String table_name = args[ 1 ];
	  
	  for ( int x = 0; x < 12; x++ ) {
		  
		  String param = args[ x ];

		  
		  if ( "-base_card".equals(param) ) {
			  
			  strBaseCard = args[ x + 1 ];
			  
		  } else if ( "-dim_split".equals(param) ) {
			  
			  strDimPerSplit = args[ x + 1 ];
			  
		  } else if ( "-base_word_len".equals(param) ) {
			  
			  strBaseWordLen = args[ x + 1 ];
			  
		  } else if ( "-base_ts_sample_size".equals(param) ) {
			  
			  strBaseTSSampleSize = args[ x + 1 ];
			  
		  } else if ( "-split_threshold".equals(param) ) {

			  strSplitThreshold = args[ x + 1 ];
			  
		  }
 
		  
		  
	  }
		
	  // need to map all of the parts to params
	  
	  ISAXIndex.CreateNewIndex( table_name, strBaseCard, strDimPerSplit, strBaseWordLen, strBaseTSSampleSize, strSplitThreshold );
		
	  //System.out.println( table_name + ", " + strBaseCard + ", " + strDimPerSplit + ", " + strBaseWordLen + ", " + strBaseTSSampleSize + ", " + strSplitThreshold );
		
	}

	public void printMainHelp() {
		
		System.out.println( "usage: Shell [MainCommand] <options>");
		System.out.println( "\tCommands");
		System.out.println( "\t\tCreateIndex // type 'Shell CreateIndex' for param list");
		System.out.println( "\t\tDeleteIndex <index_name>");
		System.out.println( "\t\tDebugIndex <index_name>");
		System.out.println( "\t\tListTables");
		System.out.println( "\t\tRunQuickTests <index_name>" );
		System.out.println( "\t\tIndexDNASample <index_name>" );
		System.out.println( "\t\tIndexGenomeFile <index_name> <genome_file>" );
		
	}
	
	public void run( String[] args ) {
		
		if ( args.length < 1 ) {
			
			this.printMainHelp();
			
		} else {
			
			int i  = 0;
			String cmd = args[ 0 ];
			
			if ( "CreateIndex".equals(cmd) ) {
				
				this.CreateNewIndex(args);
				
			} else if ( "DeleteIndex".equals(cmd) ) {
				
				if ( args.length < 2 ) {
					
					this.printMainHelp();
					return;
				}
				
		    	  String table_name = args[++i];

		    	  ISAXIndex.DeleteIndex(table_name);
				
			} else if ( "DebugIndex".equals(cmd) ) {
				
				if ( args.length < 2 ) {
					
					this.printMainHelp();
					return;
				}
				
				
		    	  String table_name = args[++i];
		    	  
		    	  ISAXIndex.DebugIndex(table_name);
				
			} else if ( "ListTables".equals(cmd) ) {
				
		    	  HBaseUtils.ListTables();
		    	  
			} else if ( "IndexDNASample".equals(cmd) ) {
				
				
				
				
				if ( args.length < 2 ) {
					
					this.printMainHelp();
					return;
				}
				
		    	  String table_name = args[++i];
		    	  
		    	  System.out.println( "Quick DNA Index Test: " + table_name );
		    	  
		    	  
		    	  TestGenomicIndex.testDNASegmentInsertAndQuery( table_name );

			} else if ( "IndexGenomeFile".equals(cmd) ) {
				
				
				
				
				if ( args.length < 3 ) {
					
					this.printMainHelp();
					return;
				}
				
		    	  String table_name = args[++i];
		    	  
		    	  String genome_file = args[++i];
		    	  
		    	  
		    	  
		    	  System.out.println( "Indexing genome file" + genome_file + " into index " + table_name );
		    	  
		    	  
		    	  //TestGenomicIndex.testDNASegmentInsertAndQuery( table_name );
		    	  GenomicIndex.IndexGenomeFile(table_name, genome_file);
		    	  
		    	  
			} else if ( "RunQuickTests".equals(cmd) ) {
				
				if ( args.length < 4 ) {
					
					this.printMainHelp();
					return;
				}
				
		    	  String table_name = args[++i];
		    	  
		    	  String insertCountFlag = args[++i];
				
		    	  
		    	  
		    	  if ( insertCountFlag.equals("-n") == false ) {
		    		
		    		  this.printMainHelp();
		    		  
		    		  return;
		    	  
		    	  }
		    	  
		    	  // SAMPLE SIZE should be pulled from the index itself.
		    	  String count = args[++i];
		    	  int iCount = Integer.parseInt(count);
		    	  
		    	  System.out.println( "Quick Test: " + table_name + " with " + iCount + " inserts..." );
		    	  
		    	  
		    	  TestISAXIndex.RandomInsertAndSearchTest( table_name, iCount );
				
			}
			
		}
		
	}
	
	
	/**
	 * 
	 * Usage:
	 * 
	 * createIndex -
	 * 
	 * deleteIndex
	 * 
	 * listTables
	 * 
	 * runIndexTest
	 * 
	 * 
	 * 
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		  Shell shell = new Shell();
		  
		    int res = 0;
		    
//		    String[] args2 = { "CreateIndex", "t_Index_name", "-base_card", "4", "-dim_split", "1", "-base_word_len", "8", "-base_ts_sample_size", "32", "-split_threshold", "100" };
		    
		    shell.run(args);
	    
		    System.exit(res);		

	}	
	
	
	
}

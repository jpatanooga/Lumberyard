package tv.floe.lumberyard.hbase.isax.index.adapters;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

import edu.hawaii.jmotif.datatype.TSException;
import edu.hawaii.jmotif.datatype.Timeseries;
import edu.hawaii.jmotif.datatype.isax.ISAXUtils;
import edu.hawaii.jmotif.datatype.isax.index.TimeseriesInstance;
//import edu.hawaii.jmotif.datatype.isax.index.dev.TimeseriesInstance;
import edu.hawaii.jmotif.logic.distance.EuclideanDistance;

public class GenomePatternOccurrences extends TimeseriesInstance {

	public GenomePatternOccurrences(Timeseries ts) {
		super(ts);
		// TODO Auto-generated constructor stub
	}
	
	public String getSequence() {
		
		// confert the local ts into DNA string
		String dna = "";
		
		try {
			dna = ISAXUtils.CreateDNAFromTimeseries(this.getTS());
		} catch (TSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return dna;
		
	}
	

}

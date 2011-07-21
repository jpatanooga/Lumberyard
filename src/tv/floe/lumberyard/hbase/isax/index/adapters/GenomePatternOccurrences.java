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

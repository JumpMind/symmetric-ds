package org.jumpmind.symmetric.web.rest.model;

import java.util.ArrayList;
import java.util.List;

public class QueryResults {

	int nbrResults;
	List<Row> results;

	public QueryResults() {
		nbrResults=0;
		results = new ArrayList<Row>();
	}
	
	public int getNbrResults() {
		return nbrResults;
	}

	public void setNbrResults(int nbrResults) {
		this.nbrResults = nbrResults;
	}

	public List<Row> getResults() {
		return results;
	}

	public void setResults(List<Row> results) {
		this.results = results;
	}
	
}

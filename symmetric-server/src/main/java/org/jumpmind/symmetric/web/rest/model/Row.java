package org.jumpmind.symmetric.web.rest.model;

import java.util.ArrayList;
import java.util.List;

public class Row {

	private int rowNum;
	private List<Column> columnData;
	
	public Row() {
		columnData = new ArrayList<Column>();
	}
	
	public int getRowNum() {
		return rowNum;
	}
	public void setRowNum(int rowNum) {
		this.rowNum = rowNum;
	}
	public List<Column> getColumnData() {
		return columnData;
	}
	public void setColumData(List<Column> columData) {
		this.columnData = columData;
	}
	
}

package com.enterprise.visao360.lake.model;

public class Row {

	private String key;
	private String col1;

	public String getCol1() {
		return col1;
	}

	public void setCol1(String col1) {
		this.col1 = col1;
	}

	public Row(String key, String col1) {
		super();
		this.key = key;
		this.col1 = col1;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "Row [key=" + key + "]";
	}

}

package com.enterprise.visao360.lake.enums;

public enum TableDateFormatEnum {
	PEDT001("yyyy-MM-dd"), 
	PEDT150("yyyy-MM-dd"),
	VEDT001("yyyy-MM-dd"),
	VEDT002("yyyy-MM-dd"),
	TCDTGEN("yyyy-MM-dd"),
	T0004("yyyyMMdd"),
	T0001("yyyyMMdd"),
	T0007("yyyyMMdd"),
	TB_CRGA_FUNC("yyyy-MM-dd"),
	FORMAT_INPUT("ddMMyyyy");
	
	private final String description;
	
	/**
	 * @param description
	 */
	TableDateFormatEnum(String description){
		this.description = description;
	}


	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	
}

package com.enterprise.visao360.lake.enums;/*package com.enterprise.visao360.lake.enums;


import java.util.Arrays;
import java.util.List;

public enum MotoresEnriquecimentoEnum {
	MOTOR_CLIENTE("motorcliente"),
	
	
	public String text;

	*//**
	 * @param text
	 *//*
	private MotoresEnriquecimentoEnum(final String text) {
		this.text = text;
	}

	
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Enum#toString()
	 
	@Override
	public String toString() {
		return text;
	}

	public static List<MotoresEnriquecimentoEnum> toList() {
		List<MotoresEnriquecimentoEnum> list = null;
		list = Arrays.asList(MotoresEnriquecimentoEnum.values());
		return list;
	}

	public static boolean isValid(String motor) {

		for (MotoresEnriquecimentoEnum c : MotoresEnriquecimentoEnum.values()) {
			if (c.text.equals(motor)) {
				return true;
			}
		}
		return false;
	}
}
*/
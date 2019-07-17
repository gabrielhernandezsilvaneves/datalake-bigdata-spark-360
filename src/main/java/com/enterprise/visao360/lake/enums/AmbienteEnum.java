package com.enterprise.visao360.lake.enums;

import java.util.Arrays;
import java.util.List;


public enum AmbienteEnum {
	DEV("dev"), HK("hk"), PROD("prod");

	private final String text;

	/**
	 * @param text
	 */
	private AmbienteEnum(final String text) {
		this.text = text;
	}

	@Override
	public String toString() {
		return text;
	}

	public static List<AmbienteEnum> toList() {
		List<AmbienteEnum> list = null;
		list = Arrays.asList(AmbienteEnum.values());
		return list;
	}

	public static boolean isValid(String motor) {

		for (AmbienteEnum c : AmbienteEnum.values()) {
			if (c.text.equals(motor)) {
				return true;
			}
		}
		return false;
	}

}

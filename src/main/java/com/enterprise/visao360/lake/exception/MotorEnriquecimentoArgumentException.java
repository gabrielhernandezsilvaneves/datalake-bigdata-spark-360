package com.enterprise.visao360.lake.exception;


public class MotorEnriquecimentoArgumentException extends Exception {


	private static final long serialVersionUID = 6628909016149090051L;
	
	public MotorEnriquecimentoArgumentException(String argumentName, String message) {
		super("Argumento:" + argumentName + ", Message:" + message);
	}
	
	public MotorEnriquecimentoArgumentException(String argumentName, String message, Throwable e) {
		super("Argumento:" + argumentName + ", Message:" + message,e);
	}

}

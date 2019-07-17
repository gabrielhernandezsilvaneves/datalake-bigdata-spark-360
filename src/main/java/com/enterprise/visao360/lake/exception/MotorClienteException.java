package com.enterprise.visao360.lake.exception;

public class MotorClienteException extends Exception {
	

	private static final long serialVersionUID = 5919677413087687867L;

	/**
	 * Construtora padrao
	 * @param message
	 * @param e
	 */
	public MotorClienteException(String message, Throwable e) {
		super(message,e);
	}

}

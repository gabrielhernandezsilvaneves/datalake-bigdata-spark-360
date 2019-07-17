package com.enterprise.visao360.lake.exception;

public class MotorEnriquecimentoConfigurationException extends Exception {

	private static final long serialVersionUID = -6766125886185522826L;

	public MotorEnriquecimentoConfigurationException(String configurationName, String message) {
		super("Configuração:" + configurationName + ", Message:" + message);
	}
	
	public MotorEnriquecimentoConfigurationException(String configurationName, String message, Throwable e) {
		super("Configuração:" + configurationName + ", Message:" + message,e);
	}
	
	public MotorEnriquecimentoConfigurationException(String configurationName, String configurationValue, String message) {
		super("Configuração:" + configurationName + ", ConfigurationValue[" + configurationValue + "] Message:" + message);
	}
	
	public MotorEnriquecimentoConfigurationException(String configurationName, String configurationValue, String message, Throwable e) {
		super("Configuração:" + configurationName + ", ConfigurationValue[" + configurationValue + "] Message:" + message,e);
	}

}
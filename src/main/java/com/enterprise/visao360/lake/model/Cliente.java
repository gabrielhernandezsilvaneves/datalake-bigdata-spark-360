package com.enterprise.visao360.lake.model;

public class Cliente {
	private String rowKey;
	private String matricula;	
	private String nomeUsuario;
	
	/**
	 * @return the rowKey
	 */
	public String getRowKey() {
		return rowKey;
	}
	/**
	 * @param rowKey the rowKey to set
	 */
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	
	
	/**
	 * @return the matricula
	 */
	public String getMatricula() {
		return matricula;
	}
	/**
	 * @param matricula the matricula to set
	 */
	public void setMatricula(String matricula) {
		this.matricula = matricula;
	}
	
		/**
	 * @return the nomeUsuario
	 */
	public String getNomeUsuario() {
		return nomeUsuario;
	}
	/**
	 * @param nomeUsuario the nomeUsuario to set
	 */
	public void setNomeUsuario(String nomeUsuario) {
		this.nomeUsuario = nomeUsuario;
	}
}
package com.enterprise.visao360.lake.domain;


public class Cliente {

	private String codigoEntidade;
	private Integer codigoCliente;
	private String nomeCliente;
	private String tipoPessoa;
	private String cpfCnpj;
	
	
	public String getCodigoEntidade() {
		return codigoEntidade;
	}

	public void setCodigoEntidade(String codigoEntidade) {
		this.codigoEntidade = codigoEntidade;
	}

	public Cliente() {}
	
	public Cliente(Integer codigoCliente,String nomeCliente, String cpfCnpj, String tipoPessoa) {
		this.codigoCliente = codigoCliente;
		this.nomeCliente = nomeCliente;
		this.cpfCnpj = cpfCnpj;
		this.tipoPessoa = tipoPessoa;
		
	}
	
	public Integer getCodigoCliente() {
		return codigoCliente;
	}
	public void setCodigoCliente(Integer codigoCliente) {
		this.codigoCliente = codigoCliente;
	}
	public String getNomeCliente() {
		return nomeCliente;
	}
	public void setNomeCliente(String nomeCliente) {
		this.nomeCliente = nomeCliente;
	}
	public String getCpfCnpj() {
		return cpfCnpj;
	}
	public void setCpfCnpj(String cpfCnpj) {
		this.cpfCnpj = cpfCnpj;
	}
	public String getTipoPessoa() {
		return tipoPessoa;
	}
	public void setTipoPessoa(String tipoPessoa) {
		this.tipoPessoa = tipoPessoa;
	}
	
}

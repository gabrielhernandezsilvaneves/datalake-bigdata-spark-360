package com.enterprise.visao360.lake.model;

import java.util.Date;
import java.util.List;

public class Fatura {

	public List<String> status__c;
	public Double valorFatura__c;
	public Double valorPago__c;
	public List<String> formaPagamento__c;
	public Date DataVencimento__c;
	public Date dataPagamento__c;
	public Long accountID;
	
	public List<String> getStatus__c() {
		return status__c;
	}
	public void setStatus__c(List<String> status__c) {
		this.status__c = status__c;
	}
	public Double getValorFatura__c() {
		return valorFatura__c;
	}
	public void setValorFatura__c(Double valorFatura__c) {
		this.valorFatura__c = valorFatura__c;
	}
	public Double getValorPago__c() {
		return valorPago__c;
	}
	public void setValorPago__c(Double valorPago__c) {
		this.valorPago__c = valorPago__c;
	}
	public List<String> getFormaPagamento__c() {
		return formaPagamento__c;
	}
	public void setFormaPagamento__c(List<String> formaPagamento__c) {
		this.formaPagamento__c = formaPagamento__c;
	}
	public Date getDataVencimento__c() {
		return DataVencimento__c;
	}
	public void setDataVencimento__c(Date dataVencimento__c) {
		DataVencimento__c = dataVencimento__c;
	}
	public Date getDataPagamento__c() {
		return dataPagamento__c;
	}
	public void setDataPagamento__c(Date dataPagamento__c) {
		this.dataPagamento__c = dataPagamento__c;
	}
	public Long getAccountID() {
		return accountID;
	}
	public void setAccountID(Long accountID) {
		this.accountID = accountID;
	}

}

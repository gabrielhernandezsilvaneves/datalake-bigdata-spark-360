package com.enterprise.visao360.lake.business; /**package com.enterprise.visao360.lake.business;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.DataFrame;

import com.enterprise.visao360.lake.config.TableConfiguration;
import com.enterprise.visao360.lake.model.Hierarquia;
import com.enterprise.frontunico.lake.model.Manager;

public interface MotorEnriquecimentoHierarquiaBusiness {

	void inserirDadosHierarquiaHazealCast(List<Manager> managerList, TableConfiguration tableConfiguration)
			throws Exception;
	
	void inserirDadosHierarquiaHbase(List<Manager> managerList, TableConfiguration tableConfiguration);
	
	List<Manager> searchManagerRedeAgencia(List<Hierarquia> clientesRedeAgencia, TableConfiguration tableConfiguration,
			DataFrame t0004df) throws IOException;

	List<Manager> searchManagerNucleoEmpresa(List<Hierarquia> nucleosEmpresa, TableConfiguration tableConfiguration) throws IOException;

	List<Manager> searchManagerAtendimentoDigital(List<Hierarquia> atendimentosDigital,
			TableConfiguration tableConfiguration) throws IOException;
	

}*/

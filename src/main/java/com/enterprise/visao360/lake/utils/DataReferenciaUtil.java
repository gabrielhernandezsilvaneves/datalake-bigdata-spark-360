package com.enterprise.visao360.lake.utils;

import com.enterprise.visao360.lake.constantes.VISAO360LakeConstantes;
import com.enterprise.visao360.lake.dao.HiveDAO;
import com.enterprise.visao360.lake.exception.MotorClienteException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class DataReferenciaUtil {
	
	final static Logger logger = Logger.getLogger(DataReferenciaUtil.class);
	
	public static String obterDataReferenciaCarga(String tableName, HiveDAO hiveDao) throws MotorClienteException {
		logger.info("************************* EXECUTANDO A QUERY HIVE - ULTIMA DATA DE CARGA *** Tabela [" + tableName + "] ******************");		
		String queryDataRef=String.format(VISAO360LakeConstantes.QUERY_DATA_REF, tableName);
		logger.info("Table Data Ref: "+ tableName);		
		logger.info("Query Data Ref: "+ queryDataRef);
		DataFrame dataFrameDataRef;
		dataFrameDataRef = hiveDao.retornaDataFrame(queryDataRef);
		
		if(dataFrameDataRef==null) throw new MotorClienteException("**** Data Frame Data de Referência Nulo! ****", null);
		dataFrameDataRef.show();
		
		Row rowDataRef=dataFrameDataRef.first();
		if(rowDataRef==null) throw new MotorClienteException("**** Nenhum registro encontrado no DataFrame de Data de Referência ****", null);
		
		String dataRef=dataFrameDataRef.first().getString(0);
		
		logger.info("************************* EXECUTADA A QUERY HIVE - ULTIMA DATA DE CARGA *** Tabela [" + tableName + "] Dara [" + dataRef + "]  ****************");	
		
		return dataRef;
	}

}

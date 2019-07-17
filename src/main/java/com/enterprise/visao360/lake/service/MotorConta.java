package com.enterprise.visao360.lake.service;

import com.enterprise.visao360.lake.config.FileConfigReader;
import com.enterprise.visao360.lake.utils.ArgumentValidationUtil;
import com.enterprise.visao360.lake.config.TableConfiguration;
import com.enterprise.visao360.lake.constantes.VISAO360LakeConstantes;
import com.enterprise.visao360.lake.dao.HiveDAO;
import com.enterprise.visao360.lake.domain.ContaEntity;
import com.enterprise.visao360.lake.exception.MotorContaException;
import com.enterprise.visao360.lake.utils.DataReferenciaUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import java.util.List;

public class MotorConta {

	final static Logger logger = Logger.getLogger(MotorConta.class);
	static String resourceName = VISAO360LakeConstantes.CONFIG_MOTOR_CLIENTE;
	static String ambiente= VISAO360LakeConstantes.AMBIENTE_PADRAO;
	static String dataRefCarga="";
	
	static DataFrame resultadoContaOrigemNetUno;
	static DataFrame resultadoContaOrigemNetSMS;
	static DataFrame resultadoContaOrigemPS8;
	static DataFrame contaUnificada;
	static DataFrame resultadoPedt150;
	static HiveDAO hiveDao; // funcionalidadeDAO

	public MotorConta() throws MotorContaException {
		logger.info("Construtor da classe MotorEnriquecimentoConta");
	}

	public static void main(String[] args) throws Exception {
		logger.trace("************************* ENTROU NO MAIN ****************************");
		
		FileConfigReader confReader;
		TableConfiguration tableConfiguration;
		SparkConf sparkConf;
		JavaSparkContext jsc;
		HiveDAO hiveDao;

		try {
			
			logger.trace("******** validando argumentos de linha de comando ***************");			
			
			if (ArgumentValidationUtil.validarArgumentos(args)) {
				resourceName = args[0];
				ambiente=args[1];
				if(args.length>2)
				{
					dataRefCarga=args[2];
				}
				logger.debug("ResourceName:" + resourceName);
				logger.debug("ambiente:" + ambiente);
				logger.debug("dataRefCarga:" + dataRefCarga);
			}
			
			logger.info("************************* criando o configReader ****************************");
			confReader = new FileConfigReader(resourceName, ambiente);
			logger.info("************************* criando  tableConfiguration ****************************");
			tableConfiguration = confReader.getTableConfiguration();
			logger.info("************************* configurando o spark com os dados parametrizados  com os parametros: nome do master :::: "
							+ tableConfiguration.getMasterName() + " ****************************"
							+ "Nome da aplicacao::: " + tableConfiguration.getAppName());			
			
			logger.info("************************* configurando o spark com os dados parametrizados ****************************");
			sparkConf = new SparkConf().setMaster(tableConfiguration.getMasterName()).setAppName(tableConfiguration.getAppName());
			logger.info("************************* Spark Configuration [Start] ****************************");
			logger.info(sparkConf.toDebugString());
			logger.info("************************* Spark Configuration [End] ****************************");
			logger.info("************************* Criando  Java Spark Context ****************************");
			jsc = new JavaSparkContext(sparkConf);
			logger.info("************************* Instanciando o HiveDao****************************");
			hiveDao = new HiveDAO(jsc, Boolean.TRUE);
		} catch (Exception contaEx) {
			throw new MotorContaException("erro ao instanciar os objetos de conta", contaEx);
		}
		
		if(dataRefCarga.isEmpty()){
			 logger.info("************************* EXECUTANDO A QUERY HIVE - NAO INFORMADO VIA PARAM ************************");				
			 dataRefCarga= DataReferenciaUtil.obterDataReferenciaCarga(tableConfiguration.getTableName(), hiveDao);
		}
		else{
			logger.info("************************* EXECUTANDO A QUERY HIVE - ULTIMA DATA DE CARGA - INFORMADO VIA PARAM ************************");
		}			
		logger.info("************************* DATA DE CARGA  [" + dataRefCarga + "] ************************");

		logger.info("************************* EXECUTANDO A QUERY SELECT * FROM ****************************");
		logger.info("Query:" + tableConfiguration.getQuery());
		String queryPE = String.format(tableConfiguration.getQuery(), dataRefCarga, dataRefCarga);		
		logger.info("************************* queryPE: " +queryPE);

		resultadoContaOrigemNetUno = hiveDao.retornaDataFrame(queryPE);

		logger.info("*************************Mostrando o dataframe***************************");
		resultadoContaOrigemNetUno.show();
		
		logger.info("*************************Entrou na funcao anonima*******************************");
		
		JavaRDD<ContaEntity> contaRDDNetSms = resultadoContaOrigemNetSMS.toJavaRDD().map(new buscaHive001());
		JavaRDD<ContaEntity> contaRDDNetUno = resultadoContaOrigemNetUno.toJavaRDD().map(new buscaHive001());
		JavaRDD<ContaEntity> contaRDDPs8 = resultadoContaOrigemPS8.toJavaRDD().map(new buscaHive001());
		
		logger.info("Criando a lista para iteracao");
	
		List<ContaEntity> result1 = contaRDDNetSms.collect(); // entity do dao.
		List<ContaEntity> result2 = contaRDDNetUno.collect(); // entity do dao.
		List<ContaEntity> result3 = contaRDDPs8.collect(); // entity do dao.

		/**
		 * 
		 *  Logica de unificacao
		 * 
		 * 
		 */

		try {
			logger.info("*************************Criando Instancia DAO Cliente*******************************");
			hiveDao = new HiveDAO();
	
			System.out.println("em construcao");
			
			logger.info("*************************INICIANDO INSERCAO VISAO360*******************************");
			for (ContaEntity con : result1) {
				System.out.println("em construcao");
			}
	//	} catch (MotorContaException mce) {
	//		logger.error(mce.getMessage(), mce);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			logger.info("*************************Finalizando Batch Cliente*******************************");
			if(hiveDao!=null){				
				hiveDao=null;
			}
				
		}
		
		System.exit(0);

	}
		
	public static class buscaHive001 implements Function<Row, ContaEntity> {

		private static final long serialVersionUID = -4729184505905489424L;

		public ContaEntity call(Row row) throws Exception {
			ContaEntity conta = new ContaEntity();
			
			return conta;
		}
	}
	
}
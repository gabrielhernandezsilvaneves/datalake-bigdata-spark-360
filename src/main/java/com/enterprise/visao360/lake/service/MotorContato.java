package com.enterprise.visao360.lake.service;

import com.enterprise.visao360.lake.config.FileConfigReader;
import com.enterprise.visao360.lake.utils.ArgumentValidationUtil;
import com.enterprise.visao360.lake.config.TableConfiguration;
import com.enterprise.visao360.lake.constantes.VISAO360LakeConstantes;
import com.enterprise.visao360.lake.dao.HiveDAO;
import com.enterprise.visao360.lake.domain.ContatoEntity;
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

public class MotorContato {

	final static Logger logger = Logger.getLogger(MotorContato.class);
	static String resourceName = VISAO360LakeConstantes.CONFIG_MOTOR_CLIENTE;
	static String ambiente= VISAO360LakeConstantes.AMBIENTE_PADRAO;
	static String dataRefCarga="";
	
	static DataFrame resultadoContatoOrigemNetUno;
	static DataFrame resultadoContatoOrigemNetSMS;
	static DataFrame resultadoContatoOrigemPS8;
	static DataFrame contatoUnificado;
	static DataFrame resultadoPedt150;
	static HiveDAO hiveDao; // funcionalidadeDAO

	public MotorContato() throws MotorContaException {
		logger.info("Construtor da classe MotorEnriquecimentoContato");
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
		} catch (Exception contatoEx) {
			throw new MotorContaException("erro ao instanciar os objetos de conta", contatoEx);
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

		resultadoContatoOrigemNetUno = hiveDao.retornaDataFrame(queryPE);

		logger.info("*************************Mostrando o dataframe***************************");
		resultadoContatoOrigemNetUno.show();
		
		logger.info("*************************Entrou na funcao anonima*******************************");
		
		JavaRDD<ContatoEntity> contatoRDDNetSms = resultadoContatoOrigemNetSMS.toJavaRDD().map(new buscaHive001());
		JavaRDD<ContatoEntity> contatoRDDNetUno = resultadoContatoOrigemNetUno.toJavaRDD().map(new buscaHive001());
		JavaRDD<ContatoEntity> contatoRDDPs8 = resultadoContatoOrigemNetSMS.toJavaRDD().map(new buscaHive001());
		
		logger.info("Criando a lista para iteracao");
	
		List<ContatoEntity> result1 = contatoRDDNetSms.collect(); // entity do dao.
		List<ContatoEntity> result2 = contatoRDDNetUno.collect(); // entity do dao.
		List<ContatoEntity> result3 = contatoRDDPs8.collect(); // entity do dao.
		
		
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
			for (ContatoEntity con : result1) {
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
		
	public static class buscaHive001 implements Function<Row, ContatoEntity> {

		private static final long serialVersionUID = -4729184505905489424L;

		public ContatoEntity call(Row row) throws Exception {
			ContatoEntity contato = new ContatoEntity();
			
			return contato;
		}
	}
	
}
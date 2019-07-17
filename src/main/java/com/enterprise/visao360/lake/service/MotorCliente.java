package com.enterprise.visao360.lake.service;

import com.enterprise.visao360.lake.config.FileConfigReader;
import com.enterprise.visao360.lake.utils.ArgumentValidationUtil;
import com.enterprise.visao360.lake.config.TableConfiguration;
import com.enterprise.visao360.lake.constantes.VISAO360LakeConstantes;
import com.enterprise.visao360.lake.dao.HBaseDAO;
import com.enterprise.visao360.lake.dao.HiveDAO;
import com.enterprise.visao360.lake.exception.MotorClienteException;
import com.enterprise.visao360.lake.utils.DataReferenciaUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
//import com.enterprise.idl.visao360.dao.enriquecimento.ClienteDao;
//import com.enterprise.idl.visao360.model.ClienteEntity;

public class MotorCliente {

	final static Logger logger = Logger.getLogger(MotorCliente.class);
	static String resourceName = VISAO360LakeConstantes.CONFIG_MOTOR_CLIENTE;
	static String ambiente= VISAO360LakeConstantes.AMBIENTE_PADRAO;
	static String dataRefCarga="";
	
	static DataFrame resultadoOrigemNetUno;
	static DataFrame resultadoOrigemNetSMS;
	static DataFrame resultadoOrigemPS8;
	static DataFrame resultadoPedt150;
	static HBaseDAO hBaseDAO; // funcionalidadeDAO

	public MotorCliente() throws MotorClienteException {
		logger.info("Construtor da classe MotorEnriquecimentoCliente");
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
		} catch (Exception cliEx) {
			throw new MotorClienteException("erro ao instanciar os objetos do cliente", cliEx);
		}
		
		if(dataRefCarga.isEmpty()){
			 logger.info("************************* EXECUTANDO A QUERY HIVE - ULTIMA DATA DE CARGA - NAO INFORMADO VIA PARAM ************************");				
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

		resultadoOrigemNetUno = hiveDao.retornaDataFrame(queryPE);

		logger.info("*************************Mostrando o dataframe***************************");
		resultadoOrigemNetUno.show();
		
		logger.info("*************************Entrou na funcao anonima*******************************");
		//List<Cliente> clientesRDD = resultadoOrigemNetUno.toJavaRDD().map(new buscaHive001());
		
		logger.info("Criando a lista para iteracao");
		//List<Cliente> result = clientesRDD.collect(); // entity do dao.
		
		/*try {
			logger.info("*************************Criando Instancia DAO Cliente*******************************");
			hBaseDAO = new HBaseDAO();
			
			logger.info("*************************INICIANDO INSERÇÃO HAZEALCAST*******************************");
			for (Cliente cli : result) {
				//clienteDao.insert(cli);
			}
		} catch (MotorEnriquecimentoClienteException mce) {
			logger.error(mce.getMessage(), mce);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			logger.info("*************************Finalizando Batch Cliente*******************************");
			if(clienteDao!=null){
				clienteDao.closeConnection();
				clienteDao=null;
			}
				
		}*/
		
		System.exit(0);

	}

/*	public static class buscaHive001 implements Function<Row, ClienteEntity> {

		private static final long serialVersionUID = -4729184505905489424L;

		public ClienteEntity call(Row row) throws Exception {
			ClienteEntity cliente = new ClienteEntity();
			cliente.setCodigoEmpresaBanco(row.getString(0));
			cliente.setCodigo(row.getString(1));
			cliente.setTipoPessoa(row.getString(2));
			cliente.setNome(row.getString(3).concat(row.getString(4).concat(row.getString(5))));
			cliente.setNumeroCPFCNPJ(row.getString(6));
			return cliente;
		}
	}

	public static class buscaHive150 implements Function<Row, ClienteEntity> {

		private static final long serialVersionUID = -4729184505905489424L;

		public ClienteEntity call(Row row) throws Exception {
			ClienteEntity cliente = new ClienteEntity();
			cliente.setNumeroCPFCNPJ(row.getString(3));
			System.out.println("************************************" + row.getString(3));
			return cliente;
		}

	}*/

}

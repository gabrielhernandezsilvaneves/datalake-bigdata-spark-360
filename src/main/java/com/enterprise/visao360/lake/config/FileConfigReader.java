package com.enterprise.visao360.lake.config;

import com.enterprise.visao360.lake.exception.MotorEnriquecimentoConfigurationException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Logger;

public class FileConfigReader {

	private TableConfiguration tableConfiguration;
	final static Logger logger = Logger.getLogger(FileConfigReader.class);

	public FileConfigReader(String resourceName) throws MotorEnriquecimentoConfigurationException { readConf(resourceName + ".conf"); 	}

	public FileConfigReader(String resourceName, String ambiente) throws MotorEnriquecimentoConfigurationException { 	
		
		readConf("confs/" +ambiente + "/" + resourceName + ".conf"); 
		
	} 

	private void readConf(String resourcePath) throws MotorEnriquecimentoConfigurationException {

		logger.info("resourcePath:" + resourcePath);
		logger.info("readConf byString iniciado");
		
		Config conf = ConfigFactory.parseResources(resourcePath).getConfig("config");
		
		if (conf!=null && !conf.isEmpty())
		{
			readConf(conf);
		}
		else{
			throw new MotorEnriquecimentoConfigurationException("ConfigurationFile", resourcePath, "Arquivo de configuracao nao encontrado!");
		}
		
		logger.info("readConf byString finalizado");
		
	}
	
	private void readConf(Config conf) {
		logger.info("readConf iniciado");
		
		tableConfiguration = new TableConfiguration();
		tableConfiguration.setAppName(conf.getString("appName"));
		tableConfiguration.setColumnFamily(conf.getString("cf"));
		tableConfiguration.setDtLastRegister(conf.getString("dtLastRegister"));
		tableConfiguration.setMasterName(conf.getString("masterName"));
		tableConfiguration.setQuery(conf.getString("query"));
		tableConfiguration.setTableControleCarga(conf.getString("tableControleCarga"));
		tableConfiguration.setTableName(conf.getString("tableName"));
		tableConfiguration.setTableName1(conf.getString("tableName1"));
		tableConfiguration.setExtractionType(conf.getString("extractionType"));
		tableConfiguration.setRowKey(conf.getString("rowKey"));
		tableConfiguration.setPathKeyTab(conf.getString("pathKeyTab"));
		tableConfiguration.setUserKeyTab(conf.getString("userKeyTab"));
		tableConfiguration.setPathHbaseSite(conf.getString("pathHbaseSite"));
		tableConfiguration.setDataKey(conf.getString("dataKey"));
		tableConfiguration.setDataKey1(conf.getString("dataKey1"));
		tableConfiguration.setQueryEspecializada(conf.getString("queryEspecializada"));
		tableConfiguration.setPath1(conf.getString("path1"));
		tableConfiguration.setPath2(conf.getString("path2"));
		tableConfiguration.setPath3(conf.getString("path3"));

		logger.info("readConf finalizado");

	}



	public TableConfiguration getTableConfiguration() {
		return tableConfiguration;
	}

	public void setTableConfiguration(TableConfiguration tableConfiguration) {
		this.tableConfiguration = tableConfiguration;
	}
}

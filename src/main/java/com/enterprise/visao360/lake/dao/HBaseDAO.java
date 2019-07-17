package com.enterprise.visao360.lake.dao;

import com.enterprise.visao360.lake.constantes.VISAO360LakeConstantes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseDAO implements Serializable {
	private static final long serialVersionUID = 1L;
	final static Logger LOGGER = Logger.getLogger(HBaseDAO.class);
	
	private transient Configuration conf;

	/**
	 * @param conf
	 */
	public HBaseDAO(Configuration conf) {
		this.setConf(conf);
	}
	
	public HBaseDAO() {}

	/**
	 * @param pathHBaseSite
	 * @param pathKeyTab
	 * @param userKeyTab
	 * @return the Configuration
	 */
	public void hbaseConfig(String pathHBaseSite, String pathKeyTab, String userKeyTab) {
		Configuration conf = HBaseConfiguration.create();
		
		LOGGER.info("Path Hbase-Site: "+pathHBaseSite);
		Path hbaseSitePath = new Path(pathHBaseSite);
		conf.addResource(hbaseSitePath);
		conf.set(VISAO360LakeConstantes.SECURITY_AUTHENTICATION, VISAO360LakeConstantes.KERBEROS);
		UserGroupInformation.setConfiguration(conf);
		
		try {
			LOGGER.info("Path KeyTab: "+pathKeyTab);
			UserGroupInformation.loginUserFromKeytab(userKeyTab, pathKeyTab);
			setConf(conf);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * 
	 * Leitura de uma linha pela chave dela.
	 * 
	 * @param rowKey
	 *            Retorna uma linha diretamente usando a chave dela.
	 * 
	 */
	public Result get(String rowKey, String tableName, String family) {

		Result result = new Result();

		try (Connection connection = ConnectionFactory.createConnection(getConf());
				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))) {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(family));
			result = table.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * @param dateKey
	 * @param tableName
	 * @param family
	 * @param column
	 * @return the List<Result>
	 */
	public List<Result> scan(String dateKey, String tableName, String family, String[] column) {
		return scan(dateKey, tableName, family, null, column);
	}

	/**
	 * @param dateKey
	 * @param tableName
	 * @param family
	 * @param filter
	 * @param column
	 * @return the List<Result>
	 */
	public List<Result> scan(String dateKey, String tableName, String family, Filter filter, String[] column) {
		Scan scan = new Scan();
		for (String col : column) {
			scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
			scan.setRowPrefixFilter(Bytes.toBytes(dateKey));
			if (filter != null) {
				scan.setFilter(filter);
			}
		}
		
		LOGGER.info("AttributesMap: " +scan.getAttributesMap());

		try (Connection connection = ConnectionFactory.createConnection(getConf());
				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
				ResultScanner scanner = table.getScanner(scan)) {

			List<Result> list = new LinkedList<>();

			for (Result result : scanner) {
				list.add(result);
			}

			return list;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param map
	 * @return the void
	 */
	public void put(String tableName, String rowKey, String cf, Map<String, String> map) {
		try (Connection connection = ConnectionFactory.createConnection(getConf());
				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))) {

			Put p = new Put(Bytes.toBytes(rowKey.toString()));

			Set<String> columns = map.keySet();

			for (String column : columns) {
				String value = map.get(column);
				p.addColumn(cf.getBytes(), column.getBytes(), value.getBytes());
			}

			table.put(p);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();

		}
	}

	/**
	 * @return the conf
	 */
	public Configuration getConf() {
		return conf;
	}

	/**
	 * @param conf the conf to set
	 */
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
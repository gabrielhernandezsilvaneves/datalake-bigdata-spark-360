package com.enterprise.visao360.lake.dao;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDAO {
	
	JavaSparkContext ctx;
	SQLContext sqlContext;
	
	public HiveDAO()	{
	}
	
	public HiveDAO(JavaSparkContext jsc, boolean isParquet)	{
		this.sqlContext = new HiveContext(jsc);
		if(isParquet){
			this.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false");
		}
	}

	public DataFrame retornaDataFrame(String query) {
		DataFrame df = sqlContext.sql(query);
		return df;
	}

	public String createQueryWithParams(String sql, Object... params){
		return String.format(sql, params);
	}
}


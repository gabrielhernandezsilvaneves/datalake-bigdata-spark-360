package com.enterprise.visao360.lake.parser;

import com.enterprise.visao360.lake.model.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.LinkedList;
import java.util.List;

public class HbaseParser {

	public static String getValue(Result result, String family, String column) {
		byte[] value = result.getValue(family.getBytes(), column.getBytes());
		return Bytes.toString(value);
	}

	public static List<Row> getListRowKeys(List<Result> results) {
		List<Row> rows = new LinkedList<>();
		for (Result result : results) {
			byte[] row = result.getRow();
			if (row != null) {
				rows.add(new Row(new String(row),new String(row)));
			} else {
				System.out.println("ROW is null");
			}
		}
		return rows;

	}

	public static DataFrame toDf(SparkContext sparkContext, List<Row> data)
	{
		System.err.println("sc" + sparkContext);
		SQLContext sql = new SQLContext(sparkContext);
		DataFrame df = sql.createDataFrame(data, Row.class);

		return df;
	}	
}

 


package com.mjyutika;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(stateful = true)
public class AutoIncrementRowUDF extends UDF{

	int ctr;

	public int evaluate() {
		ctr++;
		return ctr;
	}
}




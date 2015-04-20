package com.puns.transfertohive;

import org.apache.hadoop.hive.serde2.SerDeException;

public class ColumnBuilder {

	enum NutchParsedSupport {
		URL, Datum, HTML, DatumSub;

		public static NutchParsedSupport getNutchParsedSupport(String colName)
				throws SerDeException {
			if (colName == null || colName.isEmpty()) {
				throw new SerDeException("ColName is null or empty !");
			}
			colName = colName.toLowerCase().trim();

			if (colName.equals("url")) {
				return URL;
			} else if (colName.equals("html")) {
				return HTML;
			} else if (colName.equals("datum")) {
				return Datum;
			} else if (colName.startsWith("datum")) {
				return DatumSub;
			} else {
				throw new SerDeException("Not supoprt : " + colName);
			}
		}

	}
}

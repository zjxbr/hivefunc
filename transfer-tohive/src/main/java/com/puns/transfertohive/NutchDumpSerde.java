package com.puns.transfertohive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.datanucleus.store.rdbms.sql.method.StringEqualsIgnoreCaseMethod;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * @author zjx
 *
 */
/**
 * @author zjx
 *
 */
/**
 * @author zjx
 *
 */
public class NutchDumpSerde extends AbstractSerDe {

	public static final Log LOG = LogFactory.getLog(NutchDumpSerde.class
			.getName());
	String line;
	int urlIndex;
	int datumIndex;
	int contentIndex;

	int numColumns;
	String inputRegex;

	//
	Pattern inputPattern;
	//
	StructObjectInspector rowOI;
	List<Object> row;
	List<TypeInfo> columnTypes;
	Object[] outputFields;
	Text outputRowText;

	boolean alreadyLoggedNoMatch = false;
	boolean alreadyLoggedPartialMatch = false;

	private int counter = 0;

	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		// We can get the table definition from tbl.

		// Read the configuration parameters
		// we do not need this
		// inputRegex = tbl.getProperty("input.regex");
		String columnNameProperty = tbl
				.getProperty(serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tbl
				.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		boolean inputRegexIgnoreCase = "true".equalsIgnoreCase(tbl
				.getProperty("input.regex.case.insensitive"));

		// output format string is not supported anymore, warn user of
		// deprecation

		// Parse the configuration parameters
		List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		columnTypes = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);
		assert columnNames.size() == columnTypes.size();
		numColumns = columnNames.size();

		/*
		 * Constructing the row ObjectInspector: The row consists of some set of
		 * primitive columns, each column will be a java object of primitive
		 * type.
		 */
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
				columnNames.size());
		for (int c = 0; c < numColumns; c++) {
			TypeInfo typeInfo = columnTypes.get(c);
			if (typeInfo instanceof PrimitiveTypeInfo) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
				AbstractPrimitiveJavaObjectInspector oi = PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector(pti);
				columnOIs.add(oi);
			} else {
				throw new SerDeException(getClass().getName()
						+ " doesn't allow column [" + c + "] named "
						+ columnNames.get(c) + " with type "
						+ columnTypes.get(c));
			}
		}

		// StandardStruct uses ArrayList to store the row.
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
				columnNames,
				columnOIs,
				Lists.newArrayList(Splitter.on('\0').split(
						tbl.getProperty("columns.comments"))));

		row = new ArrayList<Object>(numColumns);
		// Constructing the row object, etc, which will be reused for all rows.
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}
		outputFields = new Object[numColumns];
		outputRowText = new Text();

	}

	/**
	 * 验证是否存在match，并且返回下标
	 * 
	 * @param line
	 * @param match
	 * @param fromIndex
	 * @return
	 * @throws SerDeException
	 */
	private int getIndexAndExist(String line, String match, int fromIndex)
			throws SerDeException {
		int rtn = line.indexOf(match, fromIndex);
		if (rtn < 0) {
			throw new SerDeException("Should exist : " + match + " in line: "
					+ line);
		}
		return rtn + match.length();
	}

	/**
	 * 可选择性存在，如果不存在，返回-1
	 * 
	 * @param line
	 * @param match
	 * @param fromIndex
	 * @return
	 */
	private int getIndexAndOptional(String line, String match, int fromIndex) {
		int rtn = line.indexOf(match, fromIndex);
		return rtn < 0 ? rtn : rtn + match.length();

	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		line = blob.toString();
		// return if line == empty
		if (line.trim().isEmpty()) {
			// TODO
			return null;
		}
		// 1. 获取url index,必须存在
		urlIndex = this.getIndexAndExist(line, "URL:: ", 0);
		// 2.获取CrawlDatum::，必须存在
		datumIndex = this.getIndexAndExist(line, "CrawlDatum::", urlIndex);
		// 3.获取Content::，可存在可不存在，不存在证明抓取失败
		contentIndex = this.getIndexAndOptional(line, "Content::", datumIndex);

		
		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no report
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		throw new UnsupportedOperationException(
				"Nutch DeSerDe doesn't support the serialize() method");
	}

}

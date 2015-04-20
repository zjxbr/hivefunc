package com.puns.transfertohive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
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

	private static final String NUTCH_SPLIT_CONTENT = "Content::";
	private static final String NUTCH_SPLIT_CRAWL_DATUM = "CrawlDatum::";
	public static final Log LOG = LogFactory.getLog(NutchDumpSerde.class
			.getName());
	String line;
	int urlIndex;
	String strURL;
	int datumIndex;
	String strDatum;
	int contentIndex;
	String strContent;

	private final static String NUTCH_SPLIT_URL = "URL:: ";

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

	// private static final boolean[][] stats = { { true, false, },
	// {},
	// {},
	// {}};

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
		return rtn;
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
		return line.indexOf(match, fromIndex);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		// 逻辑：
		// 1.如果为空，----
		// 2.Nutch很奇怪，数据dump格式是先URL::,然后有可能是"CrawlDatum::",也有可能是"Content::"
		// 但是"CrawlDatum::"一定会有。
		// 3.逻辑如下
		// 3.1 取出URL::,如果找不到报错
		// 3.2 取出CrawlDatum::,如果找不到报错
		// 3.3 取出Content::，如果找不到，没事，如果找到了，和CrawlDatum比大小。然后决定取出顺序

		if (LOG.isDebugEnabled()) {
			LOG.debug(blob.toString());
		}

		line = blob.toString();
		// return if line == empty
		if (line.trim().isEmpty()) {
			// TODO
			return null;
		}

		// 1.1. 获取url index,必须存在
		urlIndex = this.getIndexAndExist(line, NUTCH_SPLIT_URL, 0);
		// 1.2.获取CrawlDatum::，必须存在
		datumIndex = this.getIndexAndExist(line, NUTCH_SPLIT_CRAWL_DATUM,
				urlIndex); // 1.3.获取Content::，可存在可不存在，不存在证明抓取失败
		contentIndex = this.getIndexAndOptional(line, NUTCH_SPLIT_CONTENT,
				urlIndex);
		if (contentIndex < 0) {
			// 不存在content
			strURL = line.substring(urlIndex + NUTCH_SPLIT_URL.length(),
					datumIndex - 1);
			strDatum = line.substring(datumIndex);
			strContent = "";
		} else {
			if (contentIndex > datumIndex) {
				// contentIndex在后
				strURL = line.substring(urlIndex + NUTCH_SPLIT_URL.length(),
						datumIndex - 1);
				strDatum = line.substring(
						datumIndex + NUTCH_SPLIT_CRAWL_DATUM.length(),
						contentIndex - 1);
				strContent = line.substring(contentIndex
						+ NUTCH_SPLIT_CONTENT.length());
			} else {
				// contentIndex在前
				strURL = line.substring(urlIndex + NUTCH_SPLIT_URL.length(),
						contentIndex - 1);
				strDatum = line.substring(datumIndex
						+ NUTCH_SPLIT_CRAWL_DATUM.length());
				strContent = line.substring(
						contentIndex + NUTCH_SPLIT_CONTENT.length(),
						datumIndex - 1);

			}

		}

		// 对url trim
		row.set(0, strURL.trim());
		// 对datum做replace处理
		row.set(1, strDatum.trim().replace('\n', '\001'));
		// 对html 替换所有空格
		row.set(2, strContent.trim().replaceAll("\r|\n|\r\n", ""));
		if (LOG.isDebugEnabled()) {
			LOG.debug(row);
		}
		LOG.info(counter++ + ":" + row.get(0));
		LOG.info(row.get(2));
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

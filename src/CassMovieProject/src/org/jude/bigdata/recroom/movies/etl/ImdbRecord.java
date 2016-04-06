package org.jude.bigdata.recroom.movies.etl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Represents a record read from an IMDB file destined for the no-sql DB. Some
 * of the classes that use it refer to it as JSON. It is meant to be a JSON-like
 * hierarchical key-value structure.
 * 
 * @author Tibcouser
 * 
 */
public class ImdbRecord {

	Map<String, Object> recordMap = new HashMap<String, Object>();
	String docType;

	public ImdbRecord() {
	}

	public ImdbRecord(String docType) {
		this.docType = docType;
	}

	public String getDocType() {
		return this.docType;
	}

	public void setDocType(String docType) {
		this.docType = docType;
	}

	public ImdbRecord append(String key, Object value) {
		recordMap.put(key, value);
		return this;
	}

	public ImdbRecord append(String key, String value[]) {
		recordMap.put(key, new HashSet<String>(Arrays.asList(value)));
		return this;
	}

	public ImdbRecord appendSubdoc(String key, ImdbRecord value) {
		recordMap.put(key, value);
		value.setDocType(key);
		return this;
	}

	public Object get(String key) {
		return recordMap.get(key);
	}

	public ImdbRecord getSubdoc(String key) {
		return (ImdbRecord) recordMap.get(key);
	}

	public boolean containsKey(String key) {
		return recordMap.containsKey(key);
	}

	public ImdbRecord remove(String key) {
		recordMap.remove(key);
		return this;
	}

	public String toString() {
		return recordMap.toString();
	}

	public String getMandatoryString(String key) throws ETLException {
		return (String) getMandatory(key);
	}

	public Object getMandatory(String key) throws ETLException {
		Object s = recordMap.get(key);
		if (s == null) {
			throw new ETLException(ETLConstants.ERR_FIELD_NOT_FOUND,
					"Mandatory field *" + key + "* not found in record "
							+ recordMap);
		}
		return s;
	}

	public Set<String> keys() {
		return recordMap.keySet();
	}

	/**
	 * Merge values from rec2 into this rec
	 * 
	 * @param rec2
	 */
	public void merge(ImdbRecord rec2) {
		Iterator<String> keys = rec2.keys().iterator();
		while (keys.hasNext()) {
			String nextKey = keys.next();
			this.recordMap.put(nextKey, rec2.get(nextKey));
		}
	}
}

package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.Arrays;
import java.util.HashSet;

import org.jude.bigdata.recroom.movies.etl.ETLException;
import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

/**
 * Just enough of a test file parser. Just enough.
 * @author mhavey
 *
 */
public class TestFileParser extends ImdbLineParser {

	String rows[] = null;
	int rowIdx = -1;

	public TestFileParser(String rowString) {
		rows = rowString.split("\n");
	}

	public void openReader() {
	}

	public void closeReader() {
	}

	public ImdbRecord next() {
		rowIdx++;
		if (rowIdx >= rows.length) {
			return null;
		}
		ImdbRecord rec = new ImdbRecord();
		String toks[] = rows[rowIdx].split(",");
		
		// toks:(key,value,int|text|array)+
		for (int i = 0; i < toks.length; i += 3) {
			String key = toks[i];
			ImdbRecord tokRec = rec;
			if (key.contains(".")) {
				String keyToks[] = key.split("\\.");
				if (keyToks.length != 2) {
					throw new RuntimeException("Terrible key *" + key + "*");
				}
				String doc = keyToks[0];
				key = keyToks[1];
				tokRec = rec.getSubdoc(doc);
				if (tokRec == null) {
					tokRec = new ImdbRecord();
					rec.append(doc, tokRec);					
				}
			}
			if (toks[i+2].equals("int")) {
				tokRec.append(key, Integer.parseInt(toks[i + 1]));				
			}
			else if (toks[i+2].equals("array")) {
				String arrayToks[] = toks[i+1].split("\\|");
				tokRec.append(key, new HashSet<String>(Arrays.asList(arrayToks)));				
			}
			else {
				tokRec.append(key, toks[i + 1]);								
			}
		}
		return rec;
	}

	@Override
	protected ImdbRecord parseLine(String line) throws ETLException {
		return null;
	}
}

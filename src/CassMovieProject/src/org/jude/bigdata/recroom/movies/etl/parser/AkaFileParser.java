package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;
import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

/**
 * Parses the Aka-Titles list
 * 
 * @author user
 * 
 */
public class AkaFileParser extends MultilineFileParser {
	static final String AKA_PREFIX = "   (aka ";
	static final int AKA_PREFIX_LEN = AKA_PREFIX.length();

	Logger logger = Logger.getLogger(AkaFileParser.class);
	String valuesFieldName = null;

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public AkaFileParser(String path, String keyFieldName,
			String valuesFieldName, String sourceName, String preHeaderLine,
			String headerLine, String endLine) {
		super(path, keyFieldName, true, sourceName, preHeaderLine, headerLine,
				endLine);
		this.valuesFieldName = valuesFieldName;
	}

	@Override
	protected ParseResult parseOneLine(String line, ImdbRecord currentJSON)
			throws ETLException {

		// This is an AKA line
		if (line.startsWith(AKA_PREFIX)) {
			line = line.substring(AKA_PREFIX_LEN).trim();
			int firstTab = line.indexOf("\t");
			String thisAka = line;
			if (firstTab == 0) {
				throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
						"Illegal entry in lineno " + getLineNumber()
								+ " line is *" + line + "*");
			} else if (firstTab > 0) {
				thisAka = line.substring(0, firstTab - 1);
			} else {
				thisAka = line.substring(0, line.length() - 1);
				// nothing to do. no tab, so thisAka is just the line
			}
			Set<String> existingAka = (Set<String>) (currentJSON
					.get(valuesFieldName));
			validateKey(currentJSON);
			if (thisAka.equals("...") || thisAka.startsWith("?")) {
				thisAka = "";
			}
			if (thisAka.contains("...")) {
				thisAka = thisAka.replaceAll("\\.\\.\\.", " ").trim();
			}

			if (thisAka.length() > 0) {
				if (existingAka == null) {
					existingAka = new HashSet<String>();
				}
				existingAka.add(thisAka);
				currentJSON.append(valuesFieldName, existingAka);
			}
			return new ParseResult(currentJSON, false);
		} else {
			// consider it a new movie - skip if it has garbage characters in it
			ImdbRecord newMovie = new ImdbRecord();
			newMovie.append(this.keyFieldName, line.trim());
			if (currentJSON != null && currentJSON.get(valuesFieldName) != null) {
				return new ParseResult(currentJSON, newMovie);
			}
			if (currentJSON != null) {
				logger.info("Abandoning " + currentJSON);
			}
			return new ParseResult(newMovie, false);
		}
	}
}

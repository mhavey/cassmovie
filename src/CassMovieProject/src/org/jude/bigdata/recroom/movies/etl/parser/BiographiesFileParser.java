package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

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
public class BiographiesFileParser extends MultilineFileParser {
	static final String SOURCE_NAME = "biographies";
	static final String PRE_HEADER_LINE = "BIOGRAPHY LIST";
	static final String HEADER_LINE = "==============";
	static final String END_LINE = null;

	Logger logger = Logger.getLogger(BiographiesFileParser.class);

	static final String YEAR_PATTERN = "(\\d\\d\\d\\d)";
	Pattern yearPattern = Pattern.compile(YEAR_PATTERN);

	// height: feet to metres, or is it the other way around
	static double CONVERSION = 0.39370;

	static final String HEIGHT_PATTERN = "(\\d+(\\.\\d)?)(\\s*cm\\.*)";
	static final String HEIGHT_PATTERN_FTIN = "(\\d+)(\\')(\\s*)((\\d+)(\\s1/2)?(\\\"))?";
	Pattern heightPattern = Pattern.compile(HEIGHT_PATTERN);
	Pattern heightPatternFtin = Pattern.compile(HEIGHT_PATTERN_FTIN);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public BiographiesFileParser(String path) {
		super(path, ETLConstants.FIELD_CONTRIB_ID, false, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	@Override
	protected ParseResult parseOneLine(String line, ImdbRecord currentJSON)
			throws ETLException {

		String origLine = line;

		if (line.startsWith("NM:")) {
			// consider it a new contrib
			ImdbRecord newContrib = new ImdbRecord();
			String contribName = line.substring(3).trim();
			newContrib.append(this.keyFieldName, contribName);
			return new ParseResult(newContrib, false);
		} else if (line.startsWith("DB:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			currentJSON.append(ETLConstants.FIELD_CONTRIB_BIRTH, line);

			Integer iYear = this.getYear(line);
			if (iYear != null) {
				currentJSON.append(ETLConstants.FIELD_CONTRIB_BIRTH_YEAR,
						iYear);
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("DD:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			currentJSON.append(ETLConstants.FIELD_CONTRIB_DEATH, line);

			Integer iYear = this.getYear(line);
			if (iYear != null) {
				currentJSON.append(ETLConstants.FIELD_CONTRIB_DEATH_YEAR,
						iYear);
			}

			// death cause is difficult - try getting from first bit in
			// parenetheses
			int lastParen = line.indexOf("(");
			if (lastParen >= 0) {
				int lastParenClose = line.indexOf(")", lastParen);
				if (lastParenClose > 0) {
					String cause = line
							.substring(lastParen + 1, lastParenClose);
					if (!cause.startsWith("age ")) {
						currentJSON.append(
								ETLConstants.FIELD_CONTRIB_DEATH_CAUSE, cause);
					}
				}
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("HT:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();

			String height = "";

			// try to get height in cm
			List<String> toks = super.getPatternToks(heightPattern, line);
			if (toks != null && toks.size() > 1) {
				try {
					Double.parseDouble(toks.get(0));
					height = toks.get(0);
				} catch (NumberFormatException e) {
					; // ignore
				}
			} else {

				// try ft and in.
				toks = super.getPatternToks(heightPatternFtin, line);
				if (toks != null) {
					int feet = Integer.parseInt(toks.get(0));
					double cm = feet * 12 / CONVERSION;
					if (toks.size() == 5) {
						cm += ((double) Integer.parseInt(toks.get(3)) / CONVERSION);
					} else if (toks.size() == 6) {
						cm += ((double) Integer.parseInt(toks.get(3)) + 0.5)
								/ CONVERSION;
					} else {
						if (toks.size() != 2) {
							logger.info("BAD INCHES " + toks);
						}
					}
					height = "" + cm;
				}
			}

			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("RN:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			currentJSON.append(ETLConstants.FIELD_CONTRIB_REAL_NAME, line);
			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("NK:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			addArrayItem(currentJSON, line,
					ETLConstants.FIELD_CONTRIB_NICKNAMES);
			return new ParseResult(currentJSON, false);

		} else if (line.startsWith("SP:")) {
			validateKey(currentJSON);
			line = line.substring(3).trim();
			addArrayItem(currentJSON, line, ETLConstants.FIELD_CONTRIB_SPOUSES);
			return new ParseResult(currentJSON, false);
		} else {
			validateKey(currentJSON);
			addBio(currentJSON, origLine);
			return new ParseResult(currentJSON, false);
		}
	}

	void addArrayItem(ImdbRecord json, String val, String fieldName) {
		Set<String> existing = (Set<String>) (json.get(fieldName));
		if (existing == null) {
			existing = new HashSet<String>();
		}
		existing.add(val);
		json.append(fieldName, existing);
	}

	void addBio(ImdbRecord t, String newText) {
		String currText = (String) (t.get(ETLConstants.FIELD_CONTRIB_BIO));
		if (currText == null) {
			currText = "";
		}
		currText += newText + " ";
		t.append(ETLConstants.FIELD_CONTRIB_BIO, currText);
	}

	Integer getYear(String line) {
		List<String> toks = super.getPatternToks(yearPattern, line);
		if (toks != null && toks.size() == 1) {
			return new Integer(toks.get(0));
		}
		return null;
	}
}

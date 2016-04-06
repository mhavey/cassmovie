package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

/**
 * Parses the main movie list
 * 
 * @author user
 * 
 */
public class CertificatesFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "certificates";
	static final String PRE_HEADER_LINE = "CERTIFICATES LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	protected static final String REGEX = "([^\\t]+)(\\s+)(.+)";

	// Inside Paris (2001) (V) USA:X
	// Inside Passage (2005) (TV) USA:G
	// Inside Peaches (2007) (V) UK:R18
	static Pattern pattern = Pattern.compile(REGEX);

	Logger logger = Logger.getLogger(CertificatesFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public CertificatesFileParser(String path) {
		super(pattern, ETLConstants.FIELD_CERTIFICATES, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	/**
	 * Override
	 */
	public ImdbRecord valuesJSON(String movieID, Set<String> values) {
		ImdbRecord json = new ImdbRecord();

		json.append(ETLConstants.FIELD_MOVIE_ID, movieID);
		json.append(ETLConstants.SUBDOC_PARENTAL,
				new ImdbRecord().append(valuesFieldName, values));
		return json;
	}

}

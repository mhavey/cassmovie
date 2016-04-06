package org.jude.bigdata.recroom.movies.etl.parser;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

/**
 * Parses the sound mix list
 * 
 * @author user
 * 
 */
public class SoundMixFileParser extends MultivalFileParser {
	static final String SOURCE_NAME = "sound-mix";
	static final String PRE_HEADER_LINE = "SOUND-MIX LIST";
	static final String HEADER_LINE = "===";
	static final String END_LINE = "----------------------";

	static Pattern pattern = Pattern
			.compile(MultivalFileParser.REGEX_MOVIE_PHRASE);

	// Dick Head (2000) Stereo
	// Dick Henderson (1926) De Forest Phonofilm
	// Dick Henderson (1930) Mono

	Logger logger = Logger.getLogger(SoundMixFileParser.class);

	/**
	 * Constructor. Takes file system directory path for file.
	 * 
	 * @param path
	 */
	public SoundMixFileParser(String path) {
		super(pattern, ETLConstants.FIELD_SOUNDS, path, SOURCE_NAME,
				PRE_HEADER_LINE, HEADER_LINE, END_LINE);
	}

	/**
	 * Override
	 */
	public ImdbRecord valuesJSON(String movieID, Set<String> values) {
		ImdbRecord json = new ImdbRecord();

		json.append(ETLConstants.FIELD_MOVIE_ID, movieID);
		json.append(
				ETLConstants.SUBDOC_TECHNICAL,
				new ImdbRecord().append(valuesFieldName,values));
		return json;
	}
}

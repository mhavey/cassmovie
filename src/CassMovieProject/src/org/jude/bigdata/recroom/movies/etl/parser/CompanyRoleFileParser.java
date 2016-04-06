package org.jude.bigdata.recroom.movies.etl.parser;

import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;

import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

public class CompanyRoleFileParser extends MultilineFileParser {

	/**
	 * Constructor
	 * 
	 * @param path
	 * @param source
	 * @param preHeaderLine
	 * @param headerLine
	 * @param endLine
	 */
	public CompanyRoleFileParser(String path, String source,
			String preHeaderLine, String headerLine, String endLine) {
		super(path, ETLConstants.FIELD_MOVIE_ID, true, source, preHeaderLine,
				headerLine, endLine);
	}

	/**
	 * Each line has movieID, contribName, roleDetails Can have repeating lines
	 * with same movieID, contribName
	 */
	@Override
	protected ParseResult parseOneLine(String line, ImdbRecord currentJSON)
			throws ETLException {

		// parse the line
		String toks[] = line.trim().split("\t");
		if (toks == null || toks.length < 2) {
			throw new ETLException(ETLConstants.ERR_MALFORMED_LINE,
					"Malformed line in lineno " + getLineNumber() + " "
							+ (toks == null ? "" : "" + toks.length) + "*"
							+ line + "*");
		}

		String movieID = "";
		String contribName = "";
		String role = "";
		String roleDetail = "";
		int nonbIndex = -1;
		for (int i = 0; i < toks.length; i++) {
			toks[i] = toks[i].trim();
			if (toks[i].length() == 0) {
				continue;
			}
			nonbIndex++;
			if (nonbIndex == 0) {
				movieID = toks[i];
			} else if (nonbIndex == 1) {
				contribName = toks[i];
			} else if (nonbIndex == 2) {
				role = toks[i];
			} else {
				roleDetail += toks[i] + " ";
			}
		}

		// here's my tuple
		ImdbRecord thisMovie = new ImdbRecord();
		thisMovie.append(ETLConstants.FIELD_CONTRIB_ID, contribName);
		thisMovie.append(ETLConstants.FIELD_CONTRIB_CLASS, this.sourceName);
		thisMovie.append(ETLConstants.FIELD_MOVIE_ID, movieID);
		thisMovie.append(ETLConstants.FIELD_CONTRIB_ROLE, addToCSV(null, role));
		thisMovie.append(ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
				addToCSV(null, roleDetail));
		thisMovie.append(ETLConstants.FIELD_CONTRIB_TYPE,
				ETLConstants.CONTRIB_TYPE_COMPANY);

		String currMovieID = null;
		String currContribName = null;
		if (currentJSON != null) {
			currMovieID = (String) (currentJSON
					.get(ETLConstants.FIELD_MOVIE_ID));
			currContribName = (String) (currentJSON
					.get(ETLConstants.FIELD_CONTRIB_ID));
		}
		if (currMovieID != null && currContribName != null
				&& !currMovieID.equals(movieID)
				&& !currContribName.equals(contribName)) {
			// new movie, flush the old
			return new ParseResult(currentJSON, thisMovie);
		} else {
			// still on same movie/contrib combo. just append role/role detail

			if (currentJSON != null) {
				thisMovie
						.append(ETLConstants.FIELD_CONTRIB_ROLE,
								addToCSV(
										(String) (currentJSON
												.get(ETLConstants.FIELD_CONTRIB_ROLE)),
										(String) (thisMovie
												.get(ETLConstants.FIELD_CONTRIB_ROLE))));
				thisMovie
						.append(ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
								addToCSV(
										(String) (currentJSON
												.get(ETLConstants.FIELD_CONTRIB_ROLEDETAIL)),
										(String) (thisMovie
												.get(ETLConstants.FIELD_CONTRIB_ROLEDETAIL))));
			}
			return new ParseResult(thisMovie, false);
		}
	}
}

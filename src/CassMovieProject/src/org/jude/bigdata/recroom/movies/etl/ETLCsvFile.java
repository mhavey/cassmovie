package org.jude.bigdata.recroom.movies.etl;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * This class is a general CSV file reader-writer for use with ETL.
 * 
 * @author mhavey
 *
 */
public class ETLCsvFile {

	String fileName = null;
	FileWriter fw = null;
	int numLines = 0;
	String headers[] = null;

	Logger logger = Logger.getLogger(ETLCsvFile.class);

	/**
	 * constructor
	 * 
	 * @param fileName
	 */
	public ETLCsvFile(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * Open the CSV file for writing. Use the specified headers.
	 * 
	 * @param headers
	 * @throws ETLException
	 */
	public void openForWrite(String... headers) throws ETLException {
		if (fw != null) {
			throw new ETLException(ETLConstants.ERR_FILE, "CSV file is already open");
		}
		try {
			fw = new FileWriter(this.fileName);
			this.numLines = 0;
			this.headers = headers;

			// write the header line
			String headerLine = "";
			for (int i = 0; i < this.headers.length; i++) {
				headerLine += this.headers[i].trim();
				if (i < this.headers.length - 1) {
					headerLine += ",";
				}
			}
			writeLine(headerLine);
		} catch (IOException e) {
			safeClose();
			throw new ETLException(ETLConstants.ERR_FILE,
					"Error opening file *" + this.fileName + "* for write. Error is " + e, e);
		}
	}

	/**
	 * close file without exception; it will log for me
	 */
	public void safeClose() {
		try {
			close();
		} catch (ETLException x) {
			// swallow it - already logged
		}
	}

	/**
	 * Close the file. If it throws ETLException, it closed as much as it could
	 * before throwing the exception up.
	 * 
	 * @throws ETLException
	 */
	public void close() throws ETLException {
		ETLException savedError = null;
		if (fw != null) {
			try {
				fw.close();
				fw = null;
			} catch (Throwable e) {
				savedError = new ETLException(ETLConstants.ERR_FILE, "Error closing csv fw", e);
				ETLException.logError(logger, ETLConstants.ERR_FILE, "Error closing csv fw " + e, e);
			}
		}
		if (savedError != null) {
			throw savedError;
		}
	}

	/**
	 * Write next record to CSV file. The headers were specified at file open
	 * time. The values in the record matching the headers are written as a row.
	 * If a value is null it is left blank. If it contains a comma it is quoted.
	 * If it contains quotes each quote is prepended with a quote.
	 * 
	 * @param record
	 * @throws ETLException
	 */
	public void write(ImdbRecord record) throws ETLException {
		String dataLine = "";
		for (int i = 0; i < this.headers.length; i++) {
			Object oval = record.get(this.headers[i]);
			String sval = "";
			if (oval != null) {
				sval = oval.toString().trim();
				// escape quotes
				char schars[] = sval.toCharArray();
				StringBuffer sbval = new StringBuffer();
				boolean hasComma = false;
				for (int j = 0; j < schars.length; j++) {
					switch (schars[j]) {
					case '"':
						sbval.append("\\\"");
						break;
					case '\\':
						sbval.append("\\\\");
						break;
					case ',':
						hasComma = true;
						// fall through
					default:
						sbval.append(schars[j]);
						break;
					}
				}
				if (hasComma) {
					sval = '"' + sbval.toString() + '"';
				} else {
					sval = sbval.toString();
				}
				dataLine += sval;
				if (i < this.headers.length - 1) {
					dataLine += ",";
				}
			}
		}
		try {
			writeLine(dataLine);
		} catch (IOException e) {
			throw new ETLException(ETLConstants.ERR_FILE,
					"Error writing line *" + dataLine + "* to file *" + this.fileName + "* error is " + e, e);
		}
		this.numLines++;
	}

	/**
	 * Private write liner.
	 * 
	 * @param s
	 * @throws IOException
	 */
	void writeLine(String s) throws IOException {
		fw.write(s + "\n");
	}

	/**
	 * return the number of lines in the CSV file, excluding header.
	 * 
	 * @return
	 */
	public int getNumLines() {
		return this.numLines;
	}
}

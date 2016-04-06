package org.jude.bigdata.recroom.movies.etl;

import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.parser.ImdbLineParser;

/**
 * Encapsulates next() loop to read from IMDB line parser. Key method is
 * nextRecord(). Class will cleanup implicitly - no need for caller to do it
 * 
 * @author user
 * 
 */
public class ImdbIterator {
	int numIter = 0;
	int numIterFail = 0;
	int numRejects = 0;
	int numPending = 0;

	ImdbLineParser parser = null;
	boolean eof = false;
	String source;

	Logger logger = Logger.getLogger(ImdbIterator.class);
	Logger rejectLogger = null;

	public static final int PROGRESS_AT = 100000;
	public static final long SLEEP_INTERVAL = 2000;

	/**
	 * Constructor opens the parser for the given source name
	 * 
	 * @param source
	 * @param path
	 * @throws ETLException
	 */
	public ImdbIterator(String source, String path) throws ETLException {
		this.source = source;
		this.rejectLogger = Logger.getLogger("reject." + source);
		this.parser = ImdbLineParser.getParser(source, path);
		this.parser.openReader();
	}

	/**
	 * Get next record. If null, it means either EOF or an error in the record.
	 * Check isEOF() to determine which. Also enforce the skipping logic here.
	 * 
	 * @return
	 * @throws ETLException
	 */
	public ImdbRecord nextRecord() throws ETLException {
		while (true) {
			ImdbRecord t = null;
			try {
				// read next record from IMDB source
				t = this.parser.next();
			} catch (ETLException x) {
				// This error is extremely rare. Has never occurred for me.
				ETLException.logError(logger, "Error getting next record", x);
				this.numIterFail++;
				return null;
			}

			// did I reach end-of-file?
			if (t == null) {
				this.eof = true;
				this.parser.closeReader();
				return null;
			}

			// mark where I am; mark where this record appears in the sequence
			this.numIter++;
			if ((numIter % PROGRESS_AT) == 0) {
				showProgress();
			}

			return t;
		}
	}
	
	public int getPosition() {
		return this.numIter - 1;
	}

	/**
	 * Is EOF on file?
	 * 
	 * @return
	 */
	public boolean isEOF() {
		return eof;
	}

	/**
	 * Count one CQL execution against the pending count. Iterator needs to know
	 * how outstanding/pending requests there are. For async mode only.
	 */
	public synchronized void addPending() {
		this.numPending++;
	}

	/**
	 * Log the specified record as successfully updated. Iterator will count it
	 * as successfully logged For async mode only.
	 * 
	 * @param currentRecord
	 */
	public synchronized void logSuccess() {
		this.numPending--;
	}

	/**
	 * Log that there was an error of some type (CQL execution, data integrity,
	 * business rule problem, or otherwise) processing the record. Decrement the
	 * pending update count (if async mode(
	 * 
	 * @param etlException
	 * @param currentRecord
	 * @param isPending
	 *            - if we are running in asyncMode, decrement the pending count
	 *            as a reuslt of the failure
	 * 
	 */
	public synchronized void logFailure(ETLException etlException, ImdbRecord currentRecord, boolean isPending) {

		String debugMsg = "REJECT|" + source + "|" + etlException + "|json is " + currentRecord;
		rejectLogger.error(debugMsg);
		this.numRejects++;
		if (isPending) {
			this.numPending--;
		}
	}

	/**
	 * Show where we are in the run - iterations, errors, pending records
	 */
	public void showProgress() {
		logger.info(this.source + " iter " + numIter + " iterfail " + numIterFail + " rejects " + numRejects
				+ " pending " + this.getNumPending());
	}

	/**
	 * Wait until the number of pending requests has decreased to lowerLimit.
	 * 
	 */
	public void runTo(int lowerLimit) {

		// I feel like if my limit is zero, I need to be extra sure. There's
		// always the chance I ran down to zero but another request snuck
		// in; so let's sleep a bit more and check back
		int numIter = (lowerLimit == 0 ? 3 : 1);
		for (int i = 0; i < numIter; i++) {
			while (getNumPending() > lowerLimit) {
				showProgress();
				sleep(lowerLimit);
			}
			if (i < numIter - 1) {
				sleep(lowerLimit);
			}
		}

		// whether async or not, show where we're at now.
		showProgress();

	}

	/**
	 * Returns number of of pending requests/ For async mode only.
	 */
	public synchronized int getNumPending() {
		return this.numPending;
	}

	void sleep(int lowerLimit) {
		try {
			Thread.sleep(SLEEP_INTERVAL);
		} catch (InterruptedException e) {
			logger.error("Error waiting in iterator for rundown to " + lowerLimit + ". Error is " + e, e);
		}
	}
}

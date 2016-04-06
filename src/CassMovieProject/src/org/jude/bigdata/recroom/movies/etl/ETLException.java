package org.jude.bigdata.recroom.movies.etl;

import org.apache.log4j.Logger;

public class ETLException extends Exception {

	String errCode;
	String offender;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ETLException(String errCode, String msg) {
		super(msg);
		this.errCode = errCode;
	}

	public ETLException(String errCode, String offender, String msg) {
		super(msg);
		this.errCode = errCode;
		this.offender = offender;
	}

	public ETLException(String errCode, String msg, Throwable causedBy) {
		super(msg, causedBy);
		this.errCode = errCode;
	}

	public String toString() {
		return "ETLException|" + errCode + "|" + offender + "|" + getMessage()
				+ "|" + getCause() + "|";
	}

	public String getErrCode() {
		return errCode;
	}

	public String getOffender() {
		return offender;
	}

	/**
	 * For a consistent way of logging, use this. Log error based on generic
	 * throwable.
	 * 
	 * @param logger
	 * @param code
	 * @param msg
	 * @param throwable
	 */
	public static void logError(Logger logger, String code, String msg,
			Throwable throwable) {
		throwable.printStackTrace();
		logger.error(new ETLException(code, msg).toString(), throwable);
	}

	/**
	 * For a consistent way of logging, use this. Log error based on caught
	 * ETLException
	 * 
	 * @param logger
	 * @param code
	 * @param msg
	 * @param throwable
	 */
	public static void logError(Logger logger, String extra, ETLException ex) {
		ex.printStackTrace();
		logger.error(ex.toString() + "|" + extra, ex);
	}
}

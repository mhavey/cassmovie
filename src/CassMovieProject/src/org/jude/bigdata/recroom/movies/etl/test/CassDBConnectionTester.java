package org.jude.bigdata.recroom.movies.etl.test;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLConstants;
import org.jude.bigdata.recroom.movies.etl.ETLException;
import org.jude.bigdata.recroom.movies.etl.ETLProperties;
import org.jude.bigdata.recroom.movies.etl.CassDBConnection;
import org.jude.bigdata.recroom.movies.etl.ImdbIterator;
import org.jude.bigdata.recroom.movies.etl.ImdbRecord;

public class CassDBConnectionTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ETLException {

		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		ETLProperties props = new ETLProperties();
		props.setProperty("CassNodes", "127.0.0.1");
		props.setProperty("CassKeyspace", "moviedb");
		CassDBConnection conn = new CassDBConnection();
		conn.configure(props);
		try {
			conn.connect();
			conn.createUDT(ETLConstants.SUBDOC_PARENTAL);

			conn.createStatement(ETLConstants.STMT_MOVIE,
					"insert into movie(movie_id, series_id, series_type, release_year, series_end_year, etl_status) values(?, ?, ?, ?, ?, 1) if not exists",
					ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_SERIES_ID, ETLConstants.FIELD_SERIES_TYPE,
					ETLConstants.FIELD_RELEASE_YEAR, ETLConstants.FIELD_SERIES_END_YEAR);

			conn.createStatement(ETLConstants.STMT_MOVIE_UPDATE_PREFIX + ETLConstants.SUBDOC_PARENTAL,
					"update movie set " + ETLConstants.SUBDOC_PARENTAL + "=? where movie_id=? if etl_status=1",
					ETLConstants.SUBDOC_PARENTAL, ETLConstants.FIELD_MOVIE_ID);

			conn.createStatement(
					ETLConstants.STMT_MOVIE_UPDATE_PREFIX + ETLConstants.SUBDOC_PARENTAL
							+ ETLConstants.STMT_MOVIE_UDT_SUFFIX,
					"update movie set " + ETLConstants.SUBDOC_PARENTAL + "=? where movie_id=?",
					ETLConstants.SUBDOC_PARENTAL, ETLConstants.FIELD_MOVIE_ID);

			conn.createStatement(ETLConstants.STMT_MOVIE_CHECK_PREFIX + ETLConstants.SUBDOC_PARENTAL,
					"select " + ETLConstants.SUBDOC_PARENTAL + " from movie where movie_id=?",
					ETLConstants.FIELD_MOVIE_ID)
					.addFollowonUpdateStatements(ETLConstants.STMT_MOVIE_UPDATE_PREFIX + ETLConstants.SUBDOC_PARENTAL
							+ ETLConstants.STMT_MOVIE_UDT_SUFFIX);

			conn.createStatement(ETLConstants.STMT_ROLE_CAST,
					"insert into movie_cast(movie_id, contrib_id, contrib_class, contrib_role, contrib_role_detail, release_year) values(?, ?, ?, ?, ?, ?) if not exists",
					ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_CONTRIB_ID, ETLConstants.FIELD_CONTRIB_CLASS,
					ETLConstants.FIELD_CONTRIB_ROLE, ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
					ETLConstants.FIELD_RELEASE_YEAR);
			conn.createStatement(ETLConstants.STMT_ROLE_CONTRIB,
					"insert into contributor(contrib_id, contrib_type) values(?, ?)", ETLConstants.FIELD_CONTRIB_ID,
					ETLConstants.FIELD_CONTRIB_TYPE);
			conn.createStatement(ETLConstants.STMT_ROLE_CHECK, "select release_year from movie where movie_id=?",
					ETLConstants.FIELD_MOVIE_ID)
					.addFollowonUpdateStatements(ETLConstants.STMT_ROLE_CAST, ETLConstants.STMT_ROLE_CONTRIB);

			// this should work
			ImdbIterator movieIterator = new ImdbIterator("test",
					"movie_id,aa,text,series_id,aa,text,series_type,F,text,release_year,1982,int,series_end_year,1982,int\nmovie_id,bb,text,series_id,bb,text,series_type,F,text,release_year,1985,int,series_end_year,1985,int");
			ImdbIterator udtIterator1 = new ImdbIterator("test",
					"movie_id,aa,text,parental_data.certificates,X|Y|Z,array\nmovie_id,bb,text,parental_data.certificates,L|M|N,array");
			ImdbIterator udtIterator2 = new ImdbIterator("test",
					"movie_id,aa,text,parental_data.mpaa_rating_reason,justcause,text\nmovie_id,bb,text,parental_data.mpaa_rating_reason,forthereasons,text");
			ImdbIterator roleIterator = new ImdbIterator("test",
					"movie_id,aa,text,contrib_id,mike,text,contrib_class,actor,text,contrib_role,mork,text,contrib_role_detail,mork,text\nmovie_id,aa,text,contrib_id,mary,text,contrib_class,actor,text,contrib_role,mindy,text,contrib_role_detail,mindy,text");

			while (true) {
				ImdbRecord record = movieIterator.nextRecord();
				if (record == null) {
					break;
				}
				conn.runAsyncCQL(ETLConstants.STMT_MOVIE, record, movieIterator);
			}
			movieIterator.runTo(0);

			while (true) {
				ImdbRecord record = udtIterator1.nextRecord();
				if (record == null) {
					break;
				}
				conn.runAsyncCQL(ETLConstants.STMT_MOVIE_UPDATE_PREFIX + ETLConstants.SUBDOC_PARENTAL, record,
						udtIterator1);
			}
			udtIterator1.runTo(0);

			while (true) {
				ImdbRecord record = udtIterator2.nextRecord();
				if (record == null) {
					break;
				}
				conn.runAsyncCQL(ETLConstants.STMT_MOVIE_CHECK_PREFIX + ETLConstants.SUBDOC_PARENTAL, record,
						udtIterator2);
			}
			udtIterator2.runTo(0);

			while (true) {
				ImdbRecord record = roleIterator.nextRecord();
				if (record == null) {
					break;
				}
				conn.runAsyncCQL(ETLConstants.STMT_ROLE_CHECK, record, roleIterator);
			}
			roleIterator.runTo(0);

		} finally {
			conn.disconnect();
		}
	}
}

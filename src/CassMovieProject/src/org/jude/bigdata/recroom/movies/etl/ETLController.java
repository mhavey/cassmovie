package org.jude.bigdata.recroom.movies.etl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Here's where we run ETL jobs to get data from IMDB files and write to Cass.
 * There are four types of data to load: movie, role, doc, contributor. The
 * biggest performance concern is the duration of time it takes to load all this
 * data. To make it faster, we issue CQL commands asynchronously (i.e., we use
 * executeAysnc()).
 * 
 * @author Tibcouser
 * 
 */
public class ETLController {

	static final String JOB_MOVIES = "movie";
	static final String JOB_DOCS = "doc";
	static final String JOB_ROLES = "role";
	static final String JOB_ROLE_FAST = "rolefast";
	static final String JOB_CONTRIBUTORS = "contrib";

	static final String PROPSOPT = "--props=";
	static final int PROPSOPT_LEN = PROPSOPT.length();
	static final String LOG4JOPT = "--log4j=";
	static final int LOG4JOPT_LEN = LOG4JOPT.length();
	static final String CLEANOPT = "-cleanMode";

	static final String USAGE = "USAGE: ETLController " + JOB_MOVIES + "|" + JOB_DOCS + "|" + JOB_ROLES + "|"
			+ JOB_CONTRIBUTORS + "|" + JOB_ROLE_FAST + " [<movieFile>...]" + " " + PROPSOPT + "<propsFile> " + LOG4JOPT
			+ "<propsFile> [" + CLEANOPT + "]";

	String job;
	boolean cleanMode = false;
	String propsFile = null;
	String log4jFile = null;
	List<String> imdbFiles = new ArrayList<String>();
	CassDBConnection cConnection = null;
	String imdbFilePath = null;
	String csvFilePath = null;

	Logger logger = Logger.getLogger(ETLController.class);

	/**
	 * Local class used in updating the movie table
	 * 
	 * @author Tibcouser
	 * 
	 */
	static class MovieUpdateRecord {
		String fileName;
		String tableAttribute;
		boolean isUDT;
		boolean updateUDT;

		public MovieUpdateRecord(String fileName, String tableAttribute) {
			this.fileName = fileName;
			this.tableAttribute = tableAttribute;
			this.isUDT = false;
			this.updateUDT = false;
		}

		public MovieUpdateRecord(String fileName, String tableAttribute, boolean updateUDT) {
			this(fileName, tableAttribute);
			this.isUDT = true;
			this.updateUDT = updateUDT;
		}

		public String getFileName() {
			return this.fileName;
		}

		public String getTableAttribute() {
			return this.tableAttribute;
		}

		public boolean isUDT() {
			return this.isUDT;
		}

		public boolean isUpdateUDT() {
			return this.updateUDT;
		}
	}

	/**
	 * Default constructor
	 */
	public ETLController() {
	}

	/**
	 * Configure the controller - read its properties
	 * 
	 */
	public void configure() {

		try {
			// Load props and setup Cass Connection
			ETLProperties props = new ETLProperties();
			props.loadProperties(propsFile);
			if (!this.job.equals(JOB_ROLE_FAST)) {
				// we use connection always except when doing fast role
				this.cConnection = new CassDBConnection();
				this.cConnection.configure(props);
			}
			this.imdbFilePath = props.getString(ETLConstants.PROP_IMDBPATH);
			if (this.job.equals(JOB_ROLE_FAST)) {
				this.csvFilePath = props.getString(ETLConstants.PROP_CSVPATH);
			}
			logger.info("Controller config OK");
		} catch (ETLException e) {
			ETLException.logError(logger, "Config error", e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 * We have a go! Do the requested job.
	 * 
	 */
	public void go() {
		try {
			// connect to Cass
			if (this.cConnection != null) {
				this.cConnection.connect();
			}

			String jobSpec = this.job
					+ (this.cleanMode ? " clean" : " load " + (this.imdbFiles.size() == 0 ? "" : this.imdbFiles));
			logger.info("Job start " + jobSpec);
			switch (job) {
			case JOB_MOVIES:
				if (this.cleanMode) {
					truncate(ETLConstants.TABLES_MOVIE);
				} else {
					runCreateMovies();
				}
				break;
			case JOB_DOCS:
				if (this.cleanMode) {
					truncate(ETLConstants.TABLES_DOCS);
				} else {
					runAddDocs();
				}
				break;
			case JOB_ROLES:
				if (this.cleanMode) {
					truncate(ETLConstants.TABLES_ROLES);
				} else {
					runAddRoles();
				}
				break;
			case JOB_ROLE_FAST:
				runAddRolesFast();
				break;
			case JOB_CONTRIBUTORS:
				if (this.cleanMode) {
					truncate(ETLConstants.TABLES_CONTRIBUTOR);
				} else {
					runCreateContributors();
				}
				break;
			default:
				throw new RuntimeException("Illegal job " + job);
			}
			logger.info("Job end " + jobSpec);

		} catch (ETLException e) {
			ETLException.logError(logger, "Error executing jobs", e);
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			// disconnect from Cass
			try {
				if (cConnection != null) {
					cConnection.disconnect();
				}
			} catch (ETLException de) {
				ETLException.logError(logger, "Error disconnecting from Cass", de);
				throw new RuntimeException(de.getMessage(), de);
			}
		}
	}

	void truncate(String... tableNames) throws ETLException {
		for (String tableName : tableNames) {
			this.cConnection.truncate(tableName);
		}
	}

	/**
	 * Run the create movies job
	 * 
	 * @throws ETLException
	 */
	void runCreateMovies() throws ETLException {

		// create the UDTs
		this.cConnection.createUDT(ETLConstants.SUBDOC_BUSINESS);
		this.cConnection.createUDT(ETLConstants.SUBDOC_TECHNICAL);
		this.cConnection.createUDT(ETLConstants.SUBDOC_PARENTAL);
		this.cConnection.createUDT(ETLConstants.SUBDOC_RATING);

		// create the statements to create a movie and series; just update with
		// if not exists check
		if (useImdbFile("movies")) {
			this.cConnection.createStatement(ETLConstants.STMT_MOVIE,
					"insert into movie(movie_id, series_id, series_type, release_year, series_end_year, etl_status) values(?, ?, ?, ?, ?, 1) if not exists",
					ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_SERIES_ID, ETLConstants.FIELD_SERIES_TYPE,
					ETLConstants.FIELD_RELEASE_YEAR, ETLConstants.FIELD_SERIES_END_YEAR);
			this.cConnection.createStatement(ETLConstants.STMT_SERIES,
					"insert into series(series_id, series_end_year, movie_id, release_year) values(?, ?, ?, ?)",
					ETLConstants.FIELD_SERIES_ID, ETLConstants.FIELD_SERIES_END_YEAR, ETLConstants.FIELD_MOVIE_ID,
					ETLConstants.FIELD_RELEASE_YEAR);

			// first, the main movie file; read it and dump to movie and series
			// tables.
			// also do an RI check of episode vs. series
			// A series with N episodes comes on N successive lines. There is
			// logic here to tie episode to series; it assumes this order.
			ImdbIterator creationIterator = new ImdbIterator("movies", this.imdbFilePath);
			String lastSeries = null;

			while (!creationIterator.isEOF()) {
				ImdbRecord next = creationIterator.nextRecord();
				if (next != null) {
					String series = next.getMandatoryString(ETLConstants.FIELD_SERIES_ID);
					String type = next.getMandatoryString(ETLConstants.FIELD_SERIES_TYPE);
					if (lastSeries == null || !lastSeries.equals(series)) {
						lastSeries = series;
						if (type.equals(ETLConstants.SERIES_SERIES) || type.equals(ETLConstants.SERIES_FEATURE)) {
							this.cConnection.runAsyncCQL(ETLConstants.STMT_MOVIE, next, creationIterator);
						} else {
							creationIterator.logFailure(new ETLException(ETLConstants.ERR_RECORD_SEMANTIC,
									"Movie introducing new series must be series or feature"), next, false);
						}
					} else {
						if (type.equals(ETLConstants.SERIES_EPISODE)) {
							this.cConnection.runAsyncCQL(ETLConstants.STMT_MOVIE, next, creationIterator);
							this.cConnection.runAsyncCQL(ETLConstants.STMT_SERIES, next, creationIterator);
						} else {
							creationIterator.logFailure(new ETLException(ETLConstants.ERR_RECORD_SEMANTIC,
									"Movie in same series must be an episode"), next, false);
						}
					}
				}
			}
			creationIterator.runTo(0);
		}

		// next, process each update file;
		// these files have movie updates
		// most are update with compare-and-set check for existence
		// (etl_status=1).
		// a few must do a select then udpate because they need to update a UDT
		// field.
		MovieUpdateRecord recs[] = { new MovieUpdateRecord("aka-titles", ETLConstants.FIELD_ALT_TITLES),
				new MovieUpdateRecord("business", ETLConstants.SUBDOC_BUSINESS, false),
				new MovieUpdateRecord("certificates", ETLConstants.SUBDOC_PARENTAL, false),
				new MovieUpdateRecord("color-info", ETLConstants.SUBDOC_TECHNICAL, false),
				new MovieUpdateRecord("countries", ETLConstants.FIELD_COUNTRIES),
				new MovieUpdateRecord("genres", ETLConstants.FIELD_GENRES),
				new MovieUpdateRecord("keywords", ETLConstants.FIELD_KEYWORDS),
				new MovieUpdateRecord("language", ETLConstants.FIELD_LANGUAGES),
				new MovieUpdateRecord("locations", ETLConstants.FIELD_LOCATIONS),
				new MovieUpdateRecord("ratings", ETLConstants.SUBDOC_RATING),
				new MovieUpdateRecord("running-times", ETLConstants.FIELD_RUNNING_TIME),
				new MovieUpdateRecord("mpaa-ratings-reasons", ETLConstants.SUBDOC_PARENTAL, true),
				new MovieUpdateRecord("sound-mix", ETLConstants.SUBDOC_TECHNICAL, true) };

		for (int i = 0; i < recs.length; i++) {
			if (useImdbFile(recs[i].getFileName()) || useImdbFile("movie-other")) {
				ImdbIterator updateIterator = new ImdbIterator(recs[i].getFileName(), this.imdbFilePath);

				// prepare the movie update statement
				String updateName = ETLConstants.STMT_MOVIE_UPDATE_PREFIX + recs[i].getTableAttribute();
				String cql = "update movie set " + recs[i].getTableAttribute() + "=? where movie_id=?";
				if (!recs[i].isUpdateUDT()) {
					cql += " if etl_status=1";
				} else {
					updateName += ETLConstants.STMT_MOVIE_UDT_SUFFIX;
				}
				logger.info("Movie update cql *" + cql + "*");
				this.cConnection.createStatement(updateName, cql, recs[i].getTableAttribute(),
						ETLConstants.FIELD_MOVIE_ID);

				String primarySQL = updateName;

				// prepare the movie check statement
				// if it requires updating a UDT, I need the UDT data
				if (recs[i].isUpdateUDT()) {
					cql = "select " + recs[i].getTableAttribute() + " from movie where movie_id=?";
					logger.info("Movie check cql *" + cql + "*");
					String checkName = ETLConstants.STMT_MOVIE_CHECK_PREFIX + recs[i].getTableAttribute();
					this.cConnection.createStatement(checkName, cql, ETLConstants.FIELD_MOVIE_ID)
							.addFollowonUpdateStatements(updateName);
					primarySQL = checkName;
				}

				while (!updateIterator.isEOF()) {
					ImdbRecord next = updateIterator.nextRecord();
					if (next != null) {
						this.cConnection.runAsyncCQL(primarySQL, next, updateIterator);
					}
				}
				updateIterator.runTo(0);
			}
		}
	}

	/**
	 * Run the adddocs job. This is be sync check followed by some async
	 * updates.
	 * 
	 * @throws ERLException
	 */
	void runAddDocs() throws ETLException {

		this.cConnection.createStatement(ETLConstants.STMT_DOC_POST,
				"insert into post_for_movie_or_contrib_by_type(author, subject_type, subject_id, post_id, post_content, post_type, post_subtype) values(?, 'M', ?, uuid(), ?, ?, ?)",
				ETLConstants.FIELD_DOC_AUTHOR, ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_DOC_TEXT,
				ETLConstants.FIELD_DOC_TYPE, ETLConstants.FIELD_DOC_SUBTYPE);

		this.cConnection.createStatement(ETLConstants.STMT_DOC_COUNT,
				"update posts_typecount set type_count = type_count + 1 where subject_id=? and post_type=?",
				ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_DOC_TYPE);

		this.cConnection
				.createStatement(ETLConstants.STMT_DOC_CHECK, "select release_year from movie where movie_id=?",
						ETLConstants.FIELD_MOVIE_ID)
				.addFollowonUpdateStatements(ETLConstants.STMT_DOC_POST, ETLConstants.STMT_DOC_COUNT);
		// these files have docs data
		String[] files = { "alternate-versions", "crazy-credits", "goofs", "literature", "plot", "quotes",
				"soundtracks", "taglines", "trivia" };

		for (int i = 0; i < files.length; i++) {
			if (useImdbFile(files[i])) {
				ImdbIterator iterator = new ImdbIterator(files[i], this.imdbFilePath);

				while (!iterator.isEOF()) {
					ImdbRecord next = iterator.nextRecord();
					if (next != null) {
						this.cConnection.runAsyncCQL(ETLConstants.STMT_DOC_CHECK, next, iterator);
					}
				}
				iterator.runTo(0);
			}
		}
	}

	// use these role files in a couple of places
	static final String[] roleFiles = { "actors", "actresses", "cinematographers", "composers", "costume-designers",
			"directors", "distributors", "editors", "miscellaneous-companies", "miscellaneous", "producers",
			"production-companies", "production-designers", "special-effects-companies", "writers" };
	Logger rejectLogger = null;

	/**
	 * Run the addroles job. This is the workhorse (40 million records). For
	 * each role, it first checks if the movie exists, and if it does then it
	 * proceeds with the insert. THis is probably VERY slow. As an alterative,
	 * consider the fast role load.
	 * 
	 * @throws ERLException
	 */
	void runAddRoles() throws ETLException {

		this.cConnection.createStatement(ETLConstants.STMT_ROLE_CAST,
				"insert into movie_cast(movie_id, contrib_id, contrib_class, contrib_role, contrib_role_detail, release_year) values(?, ?, ?, ?, ?, ?) if not exists",
				ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_CONTRIB_ID, ETLConstants.FIELD_CONTRIB_CLASS,
				ETLConstants.FIELD_CONTRIB_ROLE, ETLConstants.FIELD_CONTRIB_ROLEDETAIL,
				ETLConstants.FIELD_RELEASE_YEAR);

		this.cConnection.createStatement(ETLConstants.STMT_ROLE_CONTRIB,
				"insert into contributor(contrib_id, contrib_type) values(?, ?)", ETLConstants.FIELD_CONTRIB_ID,
				ETLConstants.FIELD_CONTRIB_TYPE);
		this.cConnection
				.createStatement(ETLConstants.STMT_ROLE_CHECK, "select release_year from movie where movie_id=?",
						ETLConstants.FIELD_MOVIE_ID)
				.addFollowonUpdateStatements(ETLConstants.STMT_ROLE_CAST, ETLConstants.STMT_ROLE_CONTRIB);

		for (int i = 0; i < roleFiles.length; i++) {

			// open the next file
			if (useImdbFile(roleFiles[i])) {
				ImdbIterator creationIterator = new ImdbIterator(roleFiles[i], this.imdbFilePath);
				while (!creationIterator.isEOF()) {
					ImdbRecord next = creationIterator.nextRecord();
					if (next != null) {
						this.cConnection.runAsyncCQL(ETLConstants.STMT_ROLE_CHECK, next, creationIterator);
					}
				}
				creationIterator.runTo(0);
			}
		}
	}

	/**
	 * This is step one of the fast role load. For each IMDB role file, it dumps
	 * to a CSV suitable for direct import to Cass using cqlsh.
	 */
	void runAddRolesFast() throws ETLException {

		// dump movies to CSV; we need to pass dummy
		String csvFileName = this.csvFilePath + File.separator + "movies.csv";
		ETLCsvFile csvFile = new ETLCsvFile(csvFileName);
		try {
			csvFile.openForWrite(ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_CONTRIB_ID,
					ETLConstants.FIELD_CONTRIB_CLASS, ETLConstants.FIELD_CONTRIB_ROLE,
					ETLConstants.FIELD_CONTRIB_ROLEDETAIL, ETLConstants.FIELD_CONTRIB_TYPE,
					ETLConstants.FIELD_RELEASE_YEAR, ETLConstants.FIELD_ETL_STATUS);

			ImdbIterator creationIterator = new ImdbIterator("movies", this.imdbFilePath);
			while (!creationIterator.isEOF()) {
				ImdbRecord next = creationIterator.nextRecord();
				if (next != null) {
					csvFile.write(next.append(ETLConstants.FIELD_CONTRIB_ID, "dummy")
							.append(ETLConstants.FIELD_CONTRIB_CLASS, "dummy")
							.append(ETLConstants.FIELD_CONTRIB_ROLE, "")
							.append(ETLConstants.FIELD_CONTRIB_ROLEDETAIL, "")
							.append(ETLConstants.FIELD_CONTRIB_TYPE, "").append(ETLConstants.FIELD_ETL_STATUS, "1"));
				}
			}
			creationIterator.runTo(0);
			logger.info("Saving CSV file *" + csvFileName + "* of length" + csvFile.getNumLines());

		} finally {
			csvFile.safeClose();
		}

		// dump each IMDB role file to CSV
		for (int i = 0; i < roleFiles.length; i++) {
			csvFileName = this.csvFilePath + File.separator + roleFiles[i] + ".csv";
			csvFile = new ETLCsvFile(csvFileName);

			// Writing separate contrib file with just the cols expected. Should
			// not be necessary, but cqlsh
			// copy from skipcols doesn't work as expected.
			String csvContribFileName = this.csvFilePath + File.separator + roleFiles[i] + "_contrib.csv";
			ETLCsvFile csvContribFile = new ETLCsvFile(csvContribFileName);
			try {
				csvFile.openForWrite(ETLConstants.FIELD_MOVIE_ID, ETLConstants.FIELD_CONTRIB_ID,
						ETLConstants.FIELD_CONTRIB_CLASS, ETLConstants.FIELD_CONTRIB_ROLE,
						ETLConstants.FIELD_CONTRIB_ROLEDETAIL, ETLConstants.FIELD_CONTRIB_TYPE);
				csvContribFile.openForWrite(ETLConstants.FIELD_CONTRIB_ID, ETLConstants.FIELD_CONTRIB_TYPE);

				// open the next file
				ImdbIterator creationIterator = new ImdbIterator(roleFiles[i], this.imdbFilePath);
				while (!creationIterator.isEOF()) {
					ImdbRecord next = creationIterator.nextRecord();
					if (next != null) {
						csvContribFile.write(next);
						csvFile.write(next);
					}
				}
				creationIterator.runTo(0);
				logger.info("Saving CSV file *" + csvFileName + "* of length" + csvFile.getNumLines());

			} finally {
				csvFile.safeClose();
				csvContribFile.safeClose();
			}
		}
	}

	/**
	 * Run the create contributors job These are unconstrained inserts.
	 * 
	 * @throws ERLException
	 */
	void runCreateContributors() throws ETLException {

		if (useImdbFile("aka-names")) {
			// 1. let's do aka-names first; it's the skinnier one
			this.cConnection.createStatement(ETLConstants.STMT_CONTRIB_ALIASES,
					"insert into contributor(contrib_id, contrib_type, aliases) values(?, ?, ?)",
					ETLConstants.FIELD_CONTRIB_ID, ETLConstants.FIELD_CONTRIB_TYPE, ETLConstants.FIELD_CONTRIB_ALIASES);

			ImdbIterator creationIterator = new ImdbIterator("aka-names", this.imdbFilePath);
			while (!creationIterator.isEOF()) {
				ImdbRecord next = creationIterator.nextRecord();
				if (next != null) {
					next.append(ETLConstants.FIELD_CONTRIB_TYPE, ETLConstants.CONTRIB_TYPE_PERSON);
					this.cConnection.runAsyncCQL(ETLConstants.STMT_CONTRIB_ALIASES, next, creationIterator);
				}
			}
			creationIterator.runTo(0);
		}

		// 2. next we'll add biographies, merging with aliases
		if (useImdbFile("biographies")) {
			this.cConnection.createStatement(ETLConstants.STMT_CONTRIB_BIO,
					"insert into contributor(contrib_id, contrib_type, nicknames, birth_year, birth_info, death_year, death_info, death_cause, spouses, biography) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ETLConstants.FIELD_CONTRIB_ID, ETLConstants.FIELD_CONTRIB_TYPE,
					ETLConstants.FIELD_CONTRIB_NICKNAMES, ETLConstants.FIELD_CONTRIB_BIRTH_YEAR,
					ETLConstants.FIELD_CONTRIB_BIRTH, ETLConstants.FIELD_CONTRIB_DEATH_YEAR,
					ETLConstants.FIELD_CONTRIB_DEATH, ETLConstants.FIELD_CONTRIB_DEATH_CAUSE,
					ETLConstants.FIELD_CONTRIB_SPOUSES, ETLConstants.FIELD_CONTRIB_BIO);

			ImdbIterator updateIterator = new ImdbIterator("biographies", this.imdbFilePath);
			while (!updateIterator.isEOF()) {
				ImdbRecord next = updateIterator.nextRecord();
				if (next != null) {
					next.append(ETLConstants.FIELD_CONTRIB_TYPE, ETLConstants.CONTRIB_TYPE_PERSON);
					this.cConnection.runAsyncCQL(ETLConstants.STMT_CONTRIB_BIO, next, updateIterator);
				}
			}
			updateIterator.runTo(0);
		}

	}

	boolean useImdbFile(String fileName) {
		if (this.imdbFiles.size() == 0) {
			return true;
		}
		return this.imdbFiles.contains(fileName);
	}

	/**
	 * determine the options from command line
	 * 
	 * @param args
	 */
	void processCommandLine(String args[]) {

		// determine the job
		if (args.length == 0) {
			throw new RuntimeException(USAGE);
		}
		this.job = args[0];
		if (!this.job.equals(JOB_CONTRIBUTORS) && !this.job.equals(JOB_DOCS) && !this.job.equals(JOB_MOVIES)
				&& !this.job.equals(JOB_ROLES) && !this.job.equals(JOB_ROLE_FAST)) {
			throw new RuntimeException(USAGE);
		}

		// determine the options
		boolean gotFirstOption = false;
		for (int i = 1; i < args.length; i++) {
			if (args[i].startsWith(PROPSOPT)) {
				gotFirstOption = true;
				this.propsFile = args[i].substring(PROPSOPT_LEN).trim();
			} else if (args[i].startsWith(LOG4JOPT)) {
				gotFirstOption = true;
				this.log4jFile = args[i].substring(LOG4JOPT_LEN).trim();
			} else if (args[i].equals(CLEANOPT)) {
				gotFirstOption = true;
				this.cleanMode = true;
			} else {
				if (gotFirstOption || args[i].startsWith("-")) {
					throw new RuntimeException(USAGE);
				} else {
					// add file
					this.imdbFiles.add(args[i]);
				}
			}
		}

		if (this.propsFile == null || this.log4jFile == null) {
			throw new RuntimeException(USAGE);
		}
	}

	/**
	 * 
	 * Mainline for ETL controller.
	 * 
	 * @param args
	 * @throws ETLException
	 */
	public static void main(String[] args) {
		ETLController controller = new ETLController();
		controller.processCommandLine(args);
		PropertyConfigurator.configure(controller.log4jFile);
		controller.configure();
		controller.go();
	}
}

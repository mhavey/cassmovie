package org.jude.bigdata.recroom.movies.etl;

public class ETLConstants {
	public final static String FIELD_MOVIE_ID = "movie_id";
	public final static String FIELD_SERIES_ID = "series_id";
	public final static String FIELD_SERIES_TYPE = "series_type";
	public final static String FIELD_RELEASE_YEAR = "release_year";
	public final static String FIELD_SERIES_END_YEAR = "series_end_year";
	public final static String FIELD_ALT_TITLES = "alt_titles";
	public final static String FIELD_BUDGET = "budget";
	public final static String FIELD_HIGH_GBO = "high_gbo";
	public final static String FIELD_CERTIFICATES = "certificates";
	public final static String FIELD_MPAA_RATING_REASON = "mpaa_rating_reason";
	public final static String FIELD_COLORS = "colors";
	public final static String FIELD_SOUNDS = "sounds";
	public final static String FIELD_COUNTRIES = "countries";
	public final static String FIELD_LANGUAGES = "languages";
	public final static String FIELD_GENRES = "genres";
	public final static String FIELD_GENRE = "genre";
	public final static String FIELD_KEYWORDS = "keywords";
	public final static String FIELD_LOCATIONS = "locations";
	public final static String FIELD_RATING = "rating";
	public final static String FIELD_RATING_VOTES = "rating_votes";
	public final static String FIELD_RATING_DIST = "rating_dist";
	public final static String FIELD_RUNNING_TIME = "running_time";

	public final static String FIELD_DOC_TYPE = "post_type";
	public final static String FIELD_DOC_TEXT = "post_content";
	public final static String FIELD_DOC_SUBTYPE = "post_subtype";
	public final static String FIELD_DOC_AUTHOR = "author";

	public final static String FIELD_CONTRIB_ID = "contrib_id";
	public final static String FIELD_CONTRIB_TYPE = "contrib_type";
	public final static String FIELD_CONTRIB_ALIASES = "aliases";
	public final static String FIELD_CONTRIB_BIRTH = "birth_info";
	public final static String FIELD_CONTRIB_BIRTH_YEAR = "birth_year";
	public final static String FIELD_CONTRIB_DEATH = "death_info";
	public final static String FIELD_CONTRIB_DEATH_YEAR = "death_year";
	public final static String FIELD_CONTRIB_DEATH_CAUSE = "death_cause";
	public final static String FIELD_CONTRIB_REAL_NAME = "real_name";
	public final static String FIELD_CONTRIB_SPOUSES = "spouses";
	public final static String FIELD_CONTRIB_NICKNAMES = "nicknames";
	public final static String FIELD_CONTRIB_BIO = "biography";

	public final static String FIELD_CONTRIB_CLASS = "contrib_class";
	public final static String FIELD_CONTRIB_ROLE = "contrib_role";
	public final static String FIELD_CONTRIB_ROLEDETAIL = "contrib_role_detail";

	public final static String FIELD_ETL_STATUS = "etl_status";
	
	public final static String SUBDOC_BUSINESS = "business_data";
	public final static String SUBDOC_TECHNICAL = "technical_data";
	public final static String SUBDOC_RATING = "rating_data";
	public final static String SUBDOC_PARENTAL = "parental_data";

	public static final String SERIES_FEATURE = "F";
	public static final String SERIES_SERIES = "S";
	public static final String SERIES_EPISODE = "E";

	public static final String CONTRIB_TYPE_PERSON = "P";
	public static final String CONTRIB_TYPE_COMPANY = "C";

	public static final String PROP_CASS_KEYSPACE = "CassKeyspace";
	public static final String PROP_CASS_NODES = "CassNodes";
	public static final String PROP_IMDBPATH = "IMDBPath";
	public static final String PROP_CSVPATH = "CSVPath";
	public static final String PROP_LOG4J = "LOG4J";

	public static final String STMT_MOVIE = "movie";
	public static final String STMT_SERIES = "series";
	public static final String STMT_MOVIE_CHECK_PREFIX = "movie_check_";
	public static final String STMT_MOVIE_UPDATE_PREFIX = "movie_";
	public static final String STMT_MOVIE_UDT_SUFFIX = "_udt";
	
	public static final String STMT_DOC_CHECK = "doc_check";
	public static final String STMT_DOC_POST = "doc_post";
	public static final String STMT_DOC_COUNT = " doc_count";
	
	public static final String STMT_CONTRIB_ALIASES = "contributor_aliases";
	public static final String STMT_CONTRIB_BIO = "contributor_bio";
	
	public static final String STMT_ROLE_CHECK = "role_check";
	public static final String STMT_ROLE_CAST = "role_cast";
	public static final String STMT_ROLE_CONTRIB = "role_contrib";
	public static final String STMT_ROLE_CAST_DELETE = "role_cast_delete";

	public static final String[] TABLES_MOVIE = {"movie", "series"};
	public static final String[] TABLES_DOCS = {"post_for_movie_or_contrib_by_type", "posts_typecount"};
	public static final String[] TABLES_ROLES = {"movie_cast", "role_stage"};
	public static final String[] TABLES_CONTRIBUTOR = {"contributor"};
	
	public static final String ERR_USAGE = "ProgramUsage";
	public static final String ERR_CASS = "CassandraExecution";
	public static final String ERR_MALFORMED_LINE = "MalformedLine";
	public static final String ERR_UNEXPECTED_LINE = "UnexpectedLine";
	public static final String ERR_FILE = "FileError";
	public static final String ERR_CONVERSION = "DataConversion";
	public static final String ERR_FACTORY = "ParserFactory";
	public static final String ERR_DUPE = "DuplicateRecord";
	public static final String ERR_FIELD_NOT_FOUND = "FieldNotFound";
	public static final String ERR_RECORD_NOT_FOUND = "RecordNotFound";
	public static final String ERR_STATEMENT_NOT_FOUND = "StatementNotFound";
	public static final String ERR_RECORD_SEMANTIC = "SemanticRecordError";
	public static final String ERR_SEND = "FailedToSendToDB";
}

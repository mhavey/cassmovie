CREATE KEYSPACE moviedb
WITH REPLICATION = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

use moviedb;

// represents the user ratings for a movie
CREATE TYPE rating_data (
	rating float,
	rating_votes int,
	rating_dist text
);

// represents parental ratings for a movie
CREATE TYPE parental_data (
	certificates set<text>,
	mpaa_rating_reason text
);

// represents technical details (color, sound) for a movie
CREATE TYPE technical_data (
	colors set<text>,
	sounds set<text>);

	// represents business performance data for a movie
CREATE TYPE business_data (
	budget text,
	high_gbo text
);

// the full details of a movie
CREATE TABLE movie (
	movie_id text, // ETL - movies
	series_id text, // ETL - movies
	series_type text, // ETL - movies
	release_year int, // ETL - movies
	running_time int, // ETL - running_times
	series_end_year int, // ETL - movies
	alt_titles set<text>, // ETL - aka-titles
	countries set<text>, // ETL - countries
	languages set<text>, // ETL - language
	genres set<text>, // ETL - genres
	keywords set<text>, // ETL - keywords
	locations set<text>, // ETL - locations
	technical_data frozen<technical_data>, // ETL - sound-mix, color_info
	business_data frozen<business_data>, // ETL - business
	rating_data frozen<rating_data>, // ETL - ratings
	parental_data frozen<parental_data>, // ETL - certificates, mpaa-ratings-reasons
	etl_status int, // 1=inserted
	PRIMARY KEY (movie_id)
);

// summary of episodes for a series
// most cols are episode-specific: movie_id is episode name
// release_year, running_time, keywords, plot_summary are for the episode
CREATE TABLE series (
	series_id text, // ETL - movies
	series_end_year int static, // ETL - movies
	movie_id text, // ETL - movies
	release_year int, // ETL - movies
	PRIMARY KEY (series_id, release_year, movie_id)
) WITH CLUSTERING ORDER BY ( release_year DESC, movie_id ASC );

// the full details of a contributor
CREATE TABLE contributor (
	contrib_id text, // ETL - role files
	contrib_type text, // ETL - role files determine "person" or "company"
	aliases set<text>, // ETL - aka-names
	nicknames set<text>, // ETL - biographies
	real_name text, // ETL- biographies
	birth_year int, // ETL - biographies
	birth_info text, // ETL - biographies
	death_year int, // ETL - biographies
	death_info text, // ETL - biographies
	death_cause text, // ETL-biographies
	spouses set<text>, // ETL - biographies
	biography text, // ETL - biographies
	PRIMARY KEY (contrib_id)
);

// a movie's cast
// ETL files - "actors", "actresses", "cinematographers",
// "composers", "costume-designers", "directors", "distributors",
// "editors", "miscellaneous-companies", "miscellaneous",
// "producers", "production-companies", "production-designers",
// "special-effects-companies", "writers"
create table movie_cast (
	movie_id text,
	contrib_id text,
	contrib_class text,
	contrib_role text,
	contrib_role_detail text,
	release_year int,
	PRIMARY KEY (movie_id, contrib_class, contrib_id)
) WITH CLUSTERING ORDER BY ( contrib_class ASC, contrib_id ASC );

create materialized view contrib_filmography as
	select movie_id, contrib_id, contrib_class, contrib_role, contrib_role_detail
	from movie_cast
	where movie_id is not null and contrib_id is not null and contrib_class is not null and contrib_role is not null and
	contrib_role_detail is not null and release_year is not null
	PRIMARY KEY ( contrib_id, contrib_class, release_year, movie_id )
	WITH CLUSTERING ORDER BY ( contrib_class ASC, release_year DESC );

	// a user post about a movie or contributor grouped by post type. 
// by definition we sort desc by timestamp, but with imdb data we don't generally have good timestamps
// other views can tackle the parent-child relationship
create table post_for_movie_or_contrib_by_type (
	author text,
	subject_type text static,
	subject_id text,
	parent_post_id uuid,
	post_id uuid,
	post_timestamp timestamp,
	tags set<text>,
	post_content text,
	post_type text,
	post_subtype text,
	PRIMARY KEY (subject_id, post_type, post_id)
) WITH CLUSTERING ORDER BY ( post_type asc, post_id asc );

// a count of post types per movie or contributor
// normally we will an analytics tool for such aggregations, but this aggregation is considered "operational"
create table posts_typecount (
	subject_id text,
	post_type text,
	type_count counter,
	PRIMARY KEY (subject_id, post_type)
) WITH CLUSTERING ORDER BY ( post_type asc );

// staging
create table role_stage (
	movie_id text,
	contrib_id text,
	contrib_class text,
	contrib_role text,
	contrib_role_detail text,
	contrib_type text,
	release_year int static,
	etl_status int static,
	PRIMARY KEY (movie_id, contrib_class, contrib_id)
);



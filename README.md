The following describes how to use the Cassandra movie code.

Pre-Requisite: Cassandra 3.x or higher

A) Create the table structure by running the script in ddl/movie.cql. You can run this in DataStax DevCenter or in cqlsh. 

B) Download to your local copy of the imdb directory all of the IMDB data files. You can get them from the following location: ftp.fu-berlin.de/pub/misc/movies/database. 

C) If you want to view the ETL code, import into an Eclipse workspace the project in the src/CassMovieProject directory.

D) To load the IMDB data into the Cassandra tables, run the ETL job. We provide UNIX shell scripts to drive the ETL run. You can run MOST OF the ETL from any machine that can connect to Cassandra. (If you want to run the fast role loader, you must run it from a Cassandra node.) Here are the steps: 
(i) In the bin/etl directory, modify controller.properties. Change the CassNodes property to point to your Cassandra host. To point to mulitple hosts, separate the hosts with a comma.
(ii) In the bin/etl directory, modify controller.sh. Change JAVA_HOME to point to the location of the JRE on your machine.
(iii) In the bin/etl directory, modify controller_r_fast.sh. Change CASSHOME to point to the location of Cassandra on your machine.
(iv) (On any Linux machine that can connect to Casandra) Open a shell script, cd to bin/etl, and load contributors, movies, posts, and, optionally, roles as follows
./controller_c.sh # contributors
./controller_m.sh # movies
./controller_d.sh # posts (aka docs)
./controller_r.sh  # roles. Optional. This takes a LONG TIME!!!!!!
As this runs, check controller.log and rejects.log for errors. Some rejects are to be expected. No errors should show in controller.log.
(v) (On a Cassandra host). There are two ways to load roles: via the DataStax Java driver (as in step iv), or using the cqlsh CSV loader. To use the latter approach, open a shell on a Cassandra host, cd to bin/etl, and run the following:
controller_r_fast.sh


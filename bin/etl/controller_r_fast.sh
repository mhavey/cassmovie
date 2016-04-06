CASSHOME=~/datastax-ddc-3.3.0

# role files grouped in roughly equals buckets. actors and actresses have a lot. each is its own group. 
ROLE_FILE=("composers costume-designers directors distributors miscellaneous-companies  production-companies production-designers special-effects-companies writers" "actors" "actresses" "miscellaneous producers" ) 

# remove the files
echo `date` Removing files		
for fa in "${ROLE_FILE[@]}"
do
IFS=' ' read -r -a fia <<< "$fa"
for f in "${fia[@]}"
do
rm ../../export/$f.csv
rm ../../export/"$f"_contrib.csv
done
done
rm ../../export/movies.csv
rm ../../export/role_stage_diff

# truncate role tables, including role stage
echo  `date` Truncating role and stage
./controller.sh role -cleanMode  >> role.stdout 2>>role.stderr

# recreate the role dump files
echo `date`  Running imdb to csv dump
./controller.sh rolefast>> role.stdout 2>>role.stderr

# for each group of role file, do the complete load:
for fa in "${ROLE_FILE[@]}"
do

# 1. truncate role stage
echo `date`  truncating role stage
rm ../../export/role_stage.csv
$CASSHOME/bin/cqlsh -k moviedb -e "truncate role_stage;"

# 2. Load movies to stage.
echo `date`  loading movies to stage
$CASSHOME/bin/cqlsh -k moviedb -e "copy role_stage (movie_id,contrib_id,contrib_class,contrib_role,contrib_role_detail,contrib_type,release_year,etl_status) from '../../export/movies.csv' with HEADER=true;"

IFS=' ' read -r -a fia <<< "$fa"
for f in "${fia[@]}"
do

# 3. Load each role to stage.
echo `date`  $f loading roles to stage
$CASSHOME/bin/cqlsh -k moviedb -e "copy role_stage (movie_id,contrib_id,contrib_class,contrib_role,contrib_role_detail,contrib_type) from '../../export/$f.csv' with HEADER=true"

# 4. Load each role to contrib.
echo `date`  $f loading roles to contributor
$CASSHOME/bin/cqlsh -k moviedb -e "copy contributor (contrib_id,contrib_type) from '../../export/"$f"_contrib.csv' with HEADER=true;"

done

# 5. Export role_stage
echo `date`  dumping stage
$CASSHOME/bin/cqlsh -k moviedb -e "copy role_stage(movie_id,contrib_id,contrib_role,contrib_role_detail,release_year,contrib_class,contrib_type,etl_status) to '../../export/role_stage.csv' with HEADER=true;"

# 6. Cleanse the role file based on bad records found in role_stage
echo `date`  cleansing role
# strip carriage return (td)
# dont include dummy roles (grep -v)
# include only etl_status=1 and chop off that etl_status (awk)
# for cleansing analysis, save a diff of stage before and after cleanse
echo movie_id,contrib_id,contrib_role,contrib_role_detail,release_year,contrib_class > ../../export/role_stage_cleansed.csv
tr -d '\r' < ../../export/role_stage.csv | grep -v 'dummy,,1$'  | awk '/1$/ {len=length($0); myline=substr($0, 0, len-3); print myline;}' >> ../../export/role_stage_cleansed.csv
diff ../../export/role_stage.csv ../../export/role_stage_cleansed.csv >> ../../export/role_stage_diff

# 7. Load cleansed role file to movie cast.
echo `date`  loading cleansed role
$CASSHOME/bin/cqlsh -k moviedb -e "copy movie_cast (movie_id,contrib_id,contrib_role,contrib_role_detail,release_year,contrib_class) from '../../export/role_stage_cleansed.csv' with HEADER=true;"

done




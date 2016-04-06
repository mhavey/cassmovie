./controller.sh movie -cleanMode  >> movie.stdout 2>>movie.stderr
./controller.sh movie movies >> movie.stdout 2>>movie.stderr
./controller.sh movie movie-other >> movie.stdout 2>>movie.stderr


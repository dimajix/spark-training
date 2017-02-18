/* Load data from HDFS */
text = LOAD 'alice/alice-in-wonderland.txt' AS (line:CHARARRAY);

/* Split each line into words */
tokens = FOREACH text GENERATE flatten(TOKENIZE(line)) AS word;

/* group same words together */
words = GROUP tokens BY word;

/* count tokens in every group of words */
counts = FOREACH words GENERATE group,COUNT(tokens) AS cnt;

/* Sort results by counts, so we can see top entries */
sorted = ORDER counts BY cnt DESC;

/* Store results */
STORE sorted INTO 'alice_wordcount';

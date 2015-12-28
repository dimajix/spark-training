# Pig Wordcount

## Prepare data by uploading it

    fs -mkdir alice
    fs -put /home/cloudera/data/alice/* alice

## Run Pig

    text = LOAD 'alice/alice-in-wonderland.txt' AS (line:CHARARRAY);
    tokens = FOREACH text GENERATE flatten(TOKENIZE(line)) AS word;
    words = GROUP tokens BY word;
    counts = FOREACH words GENERATE group,COUNT(tokens) AS cnt;
    sorted = ORDER counts BY cnt DESC;
    STORE sorted INTO 'alice_wordcount';

## Verify Result

    fs -cat alice_wordcount/*

## Remove Results

    fs -rm -R alice_wordcount

# Common Pig Functions

    DESCRIBE table;
    ILLUSTRATE table;


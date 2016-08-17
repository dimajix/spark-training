# Pig Wordcount

## Prepare data by uploading it

    fs -mkdir -p data/alice
    fs -put data/alice-in-wonderland.txt data/alice

## Run Pig

    text = LOAD 'data/alice/alice-in-wonderland.txt' AS (line:CHARARRAY);
    tokens = FOREACH text GENERATE flatten(TOKENIZE(line)) AS word;
    words = GROUP tokens BY word;
    counts = FOREACH words GENERATE group,COUNT(tokens) AS cnt;
    sorted = ORDER counts BY cnt DESC;
    STORE sorted INTO 'data/alice_wordcount';

## Verify Result

    fs -cat data/alice_wordcount/*

## Remove Results

    fs -rm -R data/alice_wordcount

# Common Pig Functions

    DESCRIBE table;
    ILLUSTRATE table;


-- Load text file from HDFS
lines = LOAD '/pig_example/sample.txt' AS (line:chararray);

-- Split each line into words
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Group identical words together
grouped = GROUP words BY word;

-- Count occurrences of each word
wordcount = FOREACH grouped GENERATE group AS word, COUNT(words) AS count;

-- Store results in HDFS
STORE wordcount INTO '/pig_example/output';

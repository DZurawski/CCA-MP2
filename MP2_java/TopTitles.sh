# !/bin/bash
rm -rf TopTitlesClasses
rm -rf A-output
mkdir ./TopTitlesClasses
javac -cp $(hadoop classpath) TopTitles.java -d TopTitlesClasses
jar -cvf TopTitles.jar -C TopTitlesClasses/ ./
hadoop jar TopTitles.jar TopTitles -D stopwords=stopwords.txt -D delimiters=delimiters.txt dataset/titles ./A-output
cat A-output/part-r-00000
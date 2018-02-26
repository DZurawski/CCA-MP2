# !/bin/bash
rm -rf TopTitleStatisticsClasses
rm -rf B-output
mkdir ./TopTitleStatisticsClasses
javac -cp $(hadoop classpath) TopTitleStatistics.java -d TopTitleStatisticsClasses
jar -cvf TopTitleStatistics.jar -C TopTitleStatisticsClasses/ ./
hadoop jar TopTitleStatistics.jar TopTitleStatistics -D stopwords=stopwords.txt -D delimiters=delimiters.txt dataset/titles ./B-output
cat B-output/part-r-00000
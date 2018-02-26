# !/bin/bash
rm -rf TopPopularLinksClasses
rm -rf D-output 
mkdir ./TopPopularLinksClasses
javac -cp $(hadoop classpath) TopPopularLinks.java -d TopPopularLinksClasses
jar -cvf TopPopularLinks.jar -C TopPopularLinksClasses/ ./
hadoop jar TopPopularLinks.jar TopPopularLinks dataset/links ./D-output
cat D-output/part-r-00000
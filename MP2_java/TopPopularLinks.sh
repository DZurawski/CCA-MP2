#!/bin/bash

rm -rf TopPopularLinksClasses
mkdir ./TopPopularLinksClasses
echo "====================="
javac -cp $(hadoop classpath) TopPopularLinks.java -d TopPopularLinksClasses
echo "====================="
jar -cvf TopPopularLinks.jar -C TopPopularLinksClasses/ ./
echo "====================="
hadoop jar TopPopularLinks.jar TopPopularLinks dataset/links ./D-output
echo "====================="
cat D- output/part-r-00000
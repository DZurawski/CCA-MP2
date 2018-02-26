# !/bin/bash
rm -rf PopularityLeagueClasses
rm -rf E-output
mkdir ./PopularityLeagueClasses
javac -cp $(hadoop classpath) PopularityLeague.java -d PopularityLeagueClasses
jar -cvf PopularityLeague.jar -C PopularityLeagueClasses/ ./
hadoop jar PopularityLeague.jar PopularityLeague -D league=dataset/league.txt dataset/links ./E-output
cat E-output/part-r-00000
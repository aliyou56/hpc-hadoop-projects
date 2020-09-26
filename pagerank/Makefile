
MAPREDUCE_JAR=${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.0.0.jar
HDFS_JAR=${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-client-3.0.0.jar
COMMON_JAR=${HADOOP_HOME}/share/hadoop/common/hadoop-common-3.0.0.jar
LOGGING_JAR=${HADOOP_HOME}/share/hadoop/common/lib/commons-logging-1.1.3.jar

CLASSPATH="${MAPREDUCE_JAR}:${HDFS_JAR}:${COMMON_JAR}:${LOGGING_JAR}:."

APP=pagerank
JAR=${APP}.jar
SRC=src/${APP}/

run-pr: jar
	yarn jar ${JAR} ${APP}.PageRankMR

run-ex: jar
	yarn jar ${JAR} ${APP}.ExtractLinksMR

jar: build
	jar -cfv ${JAR} -C bin . src/

build: ${SRC}*.java
	mkdir -p bin
	javac -d bin -cp ${CLASSPATH} --release 8 ${SRC}*.java

clean:
	rm -r bin/ ${JAR} 
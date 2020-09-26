
MAPREDUCE_JAR=${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.0.0.jar
HDFS_JAR=${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-client-3.0.0.jar
COMMON_JAR=${HADOOP_HOME}/share/hadoop/common/hadoop-common-3.0.0.jar
LOGGING_JAR=${HADOOP_HOME}/share/hadoop/common/lib/commons-logging-1.1.3.jar

CLASSPATH="${MAPREDUCE_JAR}:${HDFS_JAR}:${COMMON_JAR}:${LOGGING_JAR}:."

APP=rsj
JAR=${APP}.jar
SRC=src/${APP}/

run: jar
	yarn jar ${JAR} /data/stackoverflow/Users.xml /data/stackoverflow/Comments.xml join-out anti

jar: build
	jar -cfev ${JAR} ${APP}.ReduceSideJoinDriver -C bin . src/

build: ${SRC}*.java
	#mkdir -p bin
	javac -d bin -cp ${CLASSPATH} --release 8 ${SRC}*.java

clean:
	rm -rf bin/* ${JAR} 
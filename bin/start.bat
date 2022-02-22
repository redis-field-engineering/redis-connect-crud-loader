@ECHO OFF
set CLASSPATH=.
set CLASSPATH=%CLASSPATH%;"..\lib\*"
set REDIS_CONNECT_LOADER_CONFIG=..\config
set LOGBACK_CONFIG=..\config\logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "%CLASSPATH%" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredisconnect.crud.loader.configLocation=$REDIS_CONNECT_LOADER_CONFIG com.redis.connect.crud.loader.LoaderMain %1
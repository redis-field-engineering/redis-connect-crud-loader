@ECHO OFF
set CLASSPATH=.
set CLASSPATH=%CLASSPATH%;"..\lib\*"
set REDIS_CDC_LOADER_CONFIG=..\config
set LOGBACK_CONFIG=..\config\logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "%CLASSPATH%" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredislabs.cdc.loader.configLocation=$REDIS_CDC_LOADER_CONFIG com.redislabs.cdc.loader.LoaderMain %1
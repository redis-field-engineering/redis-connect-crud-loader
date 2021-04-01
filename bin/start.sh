export CLASSPATH="../lib/*"
export REDIS_CDC_LOADER_CONFIG=../config
export LOGBACK_CONFIG=../config/logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "../lib/*" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredislabs.cdc.loader.configLocation=$REDIS_CDC_LOADER_CONFIG com.redislabs.cdc.loader.LoaderMain $1
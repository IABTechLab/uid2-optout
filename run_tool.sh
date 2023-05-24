#!/bin/sh

LOGBACK_CONF=${LOGBACK_CONF:-./conf/logback.xml}

java -Djava.security.egd=file:/dev/./urandom \
    -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
    -Dlogback.configurationFile=${LOGBACK_CONF} \
    -cp ${JAR_NAME}-${JAR_VERSION}.jar \
    com.uid2.optout.tool.OptOutLogTool \
    $*

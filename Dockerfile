# sha from https://hub.docker.com/layers/amd64/eclipse-temurin/21.0.7_6-jre-alpine-3.21/images/sha256-62fa775039897e4420368514ba6c167741f6d45a0de9ff9125bee57e5aca8b75
FROM eclipse-temurin@sha256:62fa775039897e4420368514ba6c167741f6d45a0de9ff9125bee57e5aca8b75

WORKDIR /app
EXPOSE 8088

ARG JAR_NAME=uid2-optout
ARG JAR_VERSION=1.0.0-SNAPSHOT
ARG IMAGE_VERSION=1.0.0.unknownhash
ENV JAR_NAME=${JAR_NAME}
ENV JAR_VERSION=${JAR_VERSION}
ENV IMAGE_VERSION=${IMAGE_VERSION}

COPY ./target/${JAR_NAME}-${JAR_VERSION}-jar-with-dependencies.jar /app/${JAR_NAME}-${JAR_VERSION}.jar
COPY ./target/${JAR_NAME}-${JAR_VERSION}-sources.jar /app
COPY ./run_tool.sh /app
COPY ./conf/default-config.json /app/conf/
COPY ./conf/*.xml /app/conf/

RUN addgroup --gid 1100 uidusers && adduser -D -G uidusers --uid 1100 uid2-optout && mkdir -p /opt/uid2 && chmod 755 -R /opt/uid2 && mkdir -p /app && chmod 705 -R /app && mkdir -p /app/file-uploads && chmod 777 -R /app/file-uploads
USER uid2-optout

CMD java \
    -XX:MaxRAMPercentage=95 -XX:-UseCompressedOops -XX:+PrintFlagsFinal \
    -Djava.security.egd=file:/dev/./urandom \
    -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
    -Dlogback.configurationFile=/app/conf/logback.xml \
    -jar ${JAR_NAME}-${JAR_VERSION}.jar

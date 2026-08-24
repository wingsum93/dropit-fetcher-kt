# syntax=docker/dockerfile:1

FROM gradle:8.7.0-jdk21 AS builder
ARG MODULE=server

WORKDIR /home/gradle/project
COPY --chown=gradle:gradle gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY --chown=gradle:gradle gradle ./gradle
COPY --chown=gradle:gradle scraper-core ./scraper-core
COPY --chown=gradle:gradle server ./server
COPY --chown=gradle:gradle scraper-worker ./scraper-worker

RUN case "$MODULE" in server|scraper-worker) ;; *) echo "Unsupported MODULE: $MODULE"; exit 1 ;; esac
RUN chmod +x ./gradlew \
    && ./gradlew --no-daemon ":${MODULE}:shadowJar" \
    && cp "${MODULE}"/build/libs/"${MODULE}"-*.jar /tmp/app.jar

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /tmp/app.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

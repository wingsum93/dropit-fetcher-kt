# syntax=docker/dockerfile:1

FROM gradle:8.7.0-jdk21 AS builder
WORKDIR /workspace
COPY . .
RUN ./gradlew clean build

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /workspace/build/libs/*.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

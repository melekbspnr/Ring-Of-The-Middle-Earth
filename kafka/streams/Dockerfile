FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /src/target/rotr-kafka-streams-1.0.0.jar /app/rotr-kafka-streams.jar
CMD ["java", "-jar", "/app/rotr-kafka-streams.jar"]

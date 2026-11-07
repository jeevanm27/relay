# Build the jar first: mvn -B -DskipTests package   (the repo root is the Docker build context)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY relay-server/target/relay-server-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=60", "-jar", "app.jar"]

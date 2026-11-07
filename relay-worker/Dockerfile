# Build the jar first: mvn -B -DskipTests package   (the repo root is the Docker build context)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY relay-worker/target/relay-worker-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 9404
# exec form: java is PID 1 and receives SIGTERM directly, which triggers the graceful shutdown hook
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=60", "-XX:+UseSerialGC", "-jar", "app.jar"]

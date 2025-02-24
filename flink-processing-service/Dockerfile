# Use a Java 11 base image
FROM flink:1.17-java11

# Set the working directory inside the container
WORKDIR /app

# Copy the built JAR file from the target folder to the container
COPY target/flink-processing-service-1.0-jar-with-dependencies.jar target/app.jar

# Set the entry point to run the Java application
CMD ["java", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "target/app.jar"]

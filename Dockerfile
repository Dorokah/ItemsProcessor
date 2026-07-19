FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY reactive-pipeline-core/pom.xml reactive-pipeline-core/pom.xml
COPY items-processor-app/pom.xml items-processor-app/pom.xml
COPY reactive-pipeline-core reactive-pipeline-core
COPY items-processor-app items-processor-app
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -pl items-processor-app -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S spring && adduser -S spring -G spring
WORKDIR /app
COPY --from=build /workspace/items-processor-app/target/items-processor-app-*.jar app.jar
COPY pokedex.json pokedex.json
USER spring:spring
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

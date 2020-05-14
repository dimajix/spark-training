FROM openjdk:8u252-jre

CMD ["java", "-Djava.net.preferIPv4Stack=true", "-jar", "app.jar"]

ARG JAR_FILE
ADD target/${JAR_FILE} app.jar

EXPOSE 8080
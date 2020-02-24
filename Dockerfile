FROM openjdk:8-jdk-alpine
VOLUME /target
ADD target/streamshift-0.0.1-SNAPSHOT-phat.jar ./target/streamshift-0.0.1-SNAPSHOT-phat.jar
ENV KAFKA_URL=kafka://kafpoc.use-nts.com:9092
ENV DATABASE_URL=postgres://nvgesjhghrfhcu:f35411d15f0b527d3a63cfc68cf2705df912d5f266c41bce45530560f3218a0d@ec2-107-20-198-176.compute-1.amazonaws.com:5432/d7n459ov8f6val
ENV Event_Source=/streamshift-northwinds
ENV Event_Type=streamshift.salesforce.cdc
ENV KAFKA_CONSUMERGRP=demo-group
ENV KAFKA_TOPIC=dynotest
ENV SF_PASS=F@ster!21QDz0Vn407t5fk9YTSm9KGDEP
ENV SF_TOPIC=/data/ProviderEvents__chn
ENV SF_URL=https://test.salesforce.com
ENV SF_USER=rscott@northwindstech.com.ntspoc.cleanary
ENTRYPOINT [ "sh", "-c", "java -Djava.security.egd=file:/dev/./urandom -jar ./target/streamshift-0.0.1-SNAPSHOT-phat.jar" ]
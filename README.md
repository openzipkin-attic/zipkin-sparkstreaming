# zipkin-sparkstreaming
A streaming alternative to Zipkin's collector


## Experimental!

```bash
# make the jar
./mvnw clean install
# run local
java -jar ./sparkstreaming-job/target/zipkin-sparkstreaming-job-*.jar
# run remote
java -jar ./sparkstreaming-job/target/zipkin-sparkstreaming-job-*.jar --zipkin.sparkstreaming.sparkMaster=spark://acole:7077
```

# Log4J2 Appender for AWS Kinesis Stream & Firehose

## Configuration Options

### Kinesis Firehose Appender
```
<Appenders>
	<Firehose name="firehose">
    	<PatternLayout pattern="%m%n" />
		<DeliveryStreamName>YourFirehoseName</DeliveryStreamName>
		<Region>ap-northeast-1</Region>
        <!-- optional, endpoint useful for VPC -->
        <Endpoint>endpointUrl</Endpoint>
        <!-- optional, defaults to UTF-8 -->
        <Encoding>UTF-8</Encoding>
        <!-- optional, defaults to 3 -->
        <MaxRetries>3</MaxRetries>
        <!-- optional, buffer size(KB), 5 ~ 1000, defaults to 1000 -->
        <BufferSize>1000</BufferSize>
        <!-- optional, max delay time(Min.), between every firehose-put calls, 1 ~ 60, defaults to 5 -->
        <MaxPutRecordDelay>5</MaxPutRecordDelay>
	</Firehose>
</Appenders>
```
### Kinesis(Stream) Appender
```
<Appenders>
	<Kinesis name="kinesis">
    	<PatternLayout pattern="%m%n" />
		<StreamName>YourKinesisStreamName</StreamName>
		<Region>eu-west-1</Region>
        <!-- optional, endpoint useful for VPC -->
        <Endpoint>endpointUrl</Endpoint>
        <!-- optional, defaults to UTF-8 -->
        <Encoding>UTF-8</Encoding>
        <!-- optional, defaults to 3 -->
        <MaxRetries>3</MaxRetries>
        <!-- optional, buffer size(KB), 5 ~ 1000, defaults to 1000 -->
        <BufferSize>1000</BufferSize>
        <!-- optional, max delay time(Min.), between every firehose-put calls, 1 ~ 60, defaults to 5 -->
        <MaxPutRecordDelay>5</MaxPutRecordDelay>
	</Kinesis>
</Appenders>
```
### Batch Kinesis(Stream) Appender

Uploads each log line in a separate kinesis message that are batch produced together using PutRecords in the AWS Kinesis API.
Note that BufferSize param here means number of separate messages.

```
<Appenders>
    <BatchKinesis name="batch-kinesis">
        <PatternLayout pattern="%m%n" />
        <StreamName>YourKinesisStreamName</StreamName>
        <Region>eu-west-1</Region>
        <!-- optional, endpoint useful for VPC -->
        <Endpoint>endpointUrl</Endpoint>
        <!-- optional, defaults to UTF-8 -->
        <Encoding>UTF-8</Encoding>
        <!-- optional, defaults to 3 -->
        <MaxRetries>3</MaxRetries>
        <!-- optional, buffer size (no. of records(loglines)), 5 ~ 500, defaults to 100 -->
        <BufferSize>200</BufferSize>
        <!-- optional, max delay time(Min.), between every firehose-put calls, 1 ~ 60, defaults to 5 -->
        <MaxPutRecordDelay>5</MaxPutRecordDelay>
    </BatchKinesis>
</Appenders>
```
## NOTE
* Messages from Logger will be buffered first, the appender makes a put request to Kinesis only `BufferSize` or `MaxPutRecordDelay` is reached.
* The maximum size of a record sent to Kinesis is 1000 KB, if a single one message from Logger exceeds 1000 KB will be discarded. Not implementing an oversize message split to multiple records because I hope a message will not be dismantled when firehose write to S3.
* You may add the log4j-web module in web projects, see [https://logging.apache.org/log4j/2.x/manual/webapp.html]
* If projects use log4j 1.2, you need Log4j 1.2 Bridge, see [https://logging.apache.org/log4j/2.x/log4j-1.2-api/index.html]
* AWS-SDK runs `com.amazonaws.http.IdleConnectionReaper` in background, it doesn't shutdown automatically. Following code represents how a web project shutdown `IdleConnectionReaper` without add AWS-SDK jar to your deployment:

#### for Servlet 3.0 and Newer
```java
@WebListener
public class CleanUpListener implements ServletContextListener {
    @Override
    public void contextDestroyed(final ServletContextEvent sce) {
        try {
            Class.forName("com.amazonaws.http.IdleConnectionReaper").getMethod("shutdown").invoke(null);
        } catch (final Exception e) {}
        
        // or if you import AWS-SDK already, simply call
        // com.amazonaws.http.IdleConnectionReaper.shutdown();
    }
    ...
}
```
#### for Servlet 2.5 
Just remove `@WebListener` annotation from above code and add a listener element to web.xml
```
<listener>
        <listener-class>CleanUpListener</listener-class>
</listener>
```


## License, work and origin

Licensed under the Apache 2.0 License. See included LICENSE file. Original work by https://github.com/Minamoto54
Modification by Robert Hjertmann Christiansen, also under the Apache 2.0 License. 

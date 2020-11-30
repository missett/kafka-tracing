# kafka-tracing

![Scala CI](https://github.com/missett/kafka-tracing/workflows/Scala%20CI/badge.svg)

This project uses the OpenTelemetry SDK to instrument kafka consumers and producers using interceptor classes. Spans 
will be reported for every message that is consuemd and produced, with trace context passed across application
boundaries using record headers. 

TODO add a better explanation of how to plug various exporters in to the application

```
libraryDependencies += "io.github.missett" %% "kafka-tracing" % version
```

## Interceptors

The tracing is performed in kafka consumer and producer interceptors. You simply add the dependency on this package and 
then configure it with properties in your standard kafka streams/consumer/producer config object. The basic config 
required is shown below-

```
consumer.interceptor.classes = "io.github.missett.kafkatracing.interceptors.SpanReportingConsumerInterceptor"
producer.interceptor.classes = "io.github.missett.kafkatracing.interceptors.SpanReportingProducerInterceptor"
```

## Exporting

*By default the spans that your application records via the interceptors will go nowhere.*
Why is this useful? Because the OpenTelemetry spec is designed to completely separate the mechanism by which data (spans) 
is recorded in your application, from the mechanism by which that data is exported to your tool of choice. 

This means that you have to supply your own exporter. Once you have initialised the exporter within your application 
(usually somewhere around the application start point) the interceptors will automatically start using your exporter.

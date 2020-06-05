# kafka-tracing

This project is an implementation of tooling to trace kafka messages flowing through distributed systems and report the
traces to a Jaeger installation for later inspection.

The tracing is performed in kafka consumer and producer interceptors, which means that no code changes are needed in
your application. You simply add the dependency on this package and then configure it with properties in your standard
kafka streams/consumer/producer config object. Example-

```
consumer.interceptor.classes = "io.github.missett.kafkatracing.jaeger.JaegerConsumerInterceptor"
producer.interceptor.classes = "io.github.missett.kafkatracing.jaeger.JaegerProducerInterceptor"

jaeger.interceptor.service.name = "test-service"
jaeger.interceptor.tracer.tags = "test-suite=embedded-kafka-tests"
jaeger.interceptor.reporter.log.spans = "true"
jaeger.interceptor.reporter.flush.interval = "1000"
jaeger.interceptor.max.queue.size = "100"
jaeger.interceptor.sender.host = "http://localhost"
jaeger.interceptor.sender.port = "8000"
jaeger.interceptor.sender.endpoint = "http://localhost:14268/api/traces"
jaeger.interceptor.sampler.type = "remote"
jaeger.interceptor.sampler.param = "1"
jaeger.interceptor.sampler.hostport = "localhost:8001"
```

The config shown above assumes that Jaeger is available to accept reports on localhost:14268 and will sample every trace
that is created (jaeger.interceptor.sampler.param = 1). In production you would likely want to set this sampler param to
be much lower (the format is a ratio, so 0.1 = 10%) to avoid overwhelming your Jaeger system. You can use
`jaeger.interceptor.tracer.tags` to set custom tags on the spans that are reported from the application, for example you
could set a tag that reports the application version. Multiple tags can be set in a comma delimited format
(eg. `foo=bar,bar=baz`).

One of the next goals for the repo would be to simplify this configuration so that not all config options are required
and the optional ones have sensible defaults.

For local testing of your Jaeger integration there is a Makefile provided. The following commands are available.
- `make jaeger-start` - Start the local Jaeger server.
- `make jaeger-stop` - Stop and clean up the local Jaeger server.
- `make jaeger-ui` - Open the Jaeger UI in a Firefox browser (probably OSX only and probably not all OSX installations).
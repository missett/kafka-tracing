# kafka-tracing

![Scala CI](https://github.com/missett/kafka-tracing/workflows/Scala%20CI/badge.svg)

This project is an implementation of tooling to trace kafka messages flowing through distributed systems and report the
traces to a Jaeger installation for later inspection.

```
libraryDependencies += "io.github.missett" %% "kafka-tracing" % version
```

## Interceptors

The tracing is performed in kafka consumer and producer interceptors, which means that no code changes are needed in
your application. You simply add the dependency on this package and then configure it with properties in your standard
kafka streams/consumer/producer config object. The basic config required is shown below-

```
consumer.interceptor.classes = "io.github.missett.kafkatracing.jaeger.interceptors.JaegerConsumerInterceptor"
producer.interceptor.classes = "io.github.missett.kafkatracing.jaeger.interceptors.JaegerProducerInterceptor"

jaeger.interceptor.service.name = "my-service"
jaeger.interceptor.sender.type = "udp"
jaeger.interceptor.sender.host = "localhost"
jaeger.interceptor.sender.port = "14268"
```

This configures the interceptors to use UDP reporters (an HTTP alternative is available), and assumes that Jaeger is
available to accept reports on localhost:14268 via UDP. The traces will be displayed in the Jaeger UI as belonging to
service 'test-service'.

## Config

The following config options are available, shown here with their defaults and other options where applicable.

```
jaeger.interceptor.service.name = "test-service"

// may also be set to http
jaeger.interceptor.sender.type = "udp"

// host of jaeger collector, should include protocol when using http sender, eg. 'http://localhost'
jaeger.interceptor.sender.host = <must be configured by user>

// port for the jaeger collector
jaeger.interceptor.sender.port = <must be configured by user>

// applicable when using the http sender for the path that will be appended to the http url
jaeger.interceptor.sender.endpoint = ""

// comma separated list of key-value pairs to be used for tags on traces, eg 'foo=bar,bar=baz'
jaeger.interceptor.tracer.tags = ""

// configure whether the jaeger tracer should log when it reports spans
jaeger.interceptor.reporter.log.spans = "false"

jaeger.interceptor.reporter.flush.interval = "1000"
jaeger.interceptor.max.queue.size = "100"

// method used to determine whether the trace is discarded, see note on sampling below
jaeger.interceptor.sampler.type = "probabilistic"
// a param to be passed to the chosen sampler
jaeger.interceptor.sampler.param = "0.1"
// used exclusively for the remote sampler type, consult the Jaeger Java docs
jaeger.interceptor.sampler.hostport = "localhost:8001"
```

## Sampling

The tracer library initializes itself with one of a set of Sampler configurations that are available. Consult the ![Jaeger
Java Library](https://github.com/jaegertracing/jaeger-client-java/blob/master/jaeger-core/README.md).
- 'probabilistic' - a ratio of spans to be sampled vs. dropped, 0.1 is 10% of spans will be sampled
- 'const' - can be used to set all spans to be sampled or all spans to be dropped
- 'ratelimiting' - caps the rate of spans to be sent to the server at a certain rate
- 'remote' - manage the sampler remotely

## Development

For local testing of your Jaeger integration there is a Makefile provided. The following commands are available.
- `make jaeger-start` - Start the local Jaeger server.
- `make jaeger-stop` - Stop and clean up the local Jaeger server.
- `make jaeger-ui` - Open the Jaeger UI in a Firefox browser (probably OSX only and probably not all OSX installations).

Pull requests are encouraged! Open a PR against the snapshot branch to get a test version published. Once your changes
are accepted into a snapshot release a pull request will be opened against master to get a final release published.

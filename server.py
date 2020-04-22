import os
import sys
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads

from opentelemetry.ext.flask import FlaskInstrumentor
FlaskInstrumentor().instrument()
from flask import Flask, Response, stream_with_context

from opentelemetry import (
  trace,
  context,
  propagators
)

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
  SimpleExportSpanProcessor,
  BatchExportSpanProcessor,
  ConsoleSpanExporter,
)
from opentelemetry.ext.lightstep import LightStepSpanExporter
from opentelemetry.ext.jaeger import JaegerSpanExporter

trace.set_tracer_provider(TracerProvider())

serviceName = os.environ['PROJECT_NAME']

lsExporter = LightStepSpanExporter(
  name=serviceName,
  token=os.environ['LS_KEY']
)

exporter = JaegerSpanExporter(
  service_name=serviceName,
  agent_host_name=os.environ['JAEGER_HOST'],
  agent_port=6831,
)

# trace.get_tracer_provider().add_span_processor(BatchExportSpanProcessor(exporter))
trace.get_tracer_provider().add_span_processor(SimpleExportSpanProcessor(ConsoleSpanExporter()))
# trace.get_tracer_provider().add_span_processor(BatchExportSpanProcessor(lsExporter))
# trace.get_tracer_provider().add_span_processor(BatchExportSpanProcessor(hnyExporter))

tracer = trace.get_tracer(__name__)

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=[os.environ['KAFKA_HOST']], value_serializer=lambda x:dumps(x).encode('utf-8'))
consumer = KafkaConsumer(serviceName, bootstrap_servers=[os.environ['KAFKA_HOST']], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x:loads(x.decode('utf-8')))

def kafkaCarrier(carrier, key, value):
  return carrier.append((key, str.encode(value)))

def getTraceparent(headers, key):
  return [x[1].decode() for x in headers if x[0] == key]

@app.route("/")
def root():
  return "Click [Tools] > [Logs] to see spans!"

@app.route("/produce")
def producerRoute():
  for e in range(10):
    with tracer.start_as_current_span("produceData"):
      data = {'number': e}
      traceHeaders = []
      propagators.inject(kafkaCarrier, traceHeaders)
      producer.send(serviceName, value=data, headers=traceHeaders)
  return "ok"


@app.route("/consume")
def consumerRoute():
  def getMessages():
    for message in consumer:
      token = context.attach(propagators.extract(getTraceparent, message.headers))
      with tracer.start_as_current_span("consumeData"):
        yield dumps(message.value)
  return Response(stream_with_context(getMessages()))
  
if __name__ == "__main__":
  app.run(debug=True)

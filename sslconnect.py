from kafka import KafkaProducer
import ssl
import json

# set the SSL context
ssl_context = ssl.create_default_context(cafile="CARoot.pem")
ssl_context.load_cert_chain("client.crt", keyfile="client.key",password='change@it')
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
# set the producer configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka.example.com:9093'],
    security_protocol='SSL',
    ssl_context=ssl_context,
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
)

# produce a message
producer.send('GuardianOfGalaxyVol1-topic', {'message': 'hello TLS'})
producer.flush()
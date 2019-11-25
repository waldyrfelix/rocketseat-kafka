const Kafka = require('node-rdkafka');
require("dotenv").config()

function createConsumer(onData) {
    const consumer = new Kafka.KafkaConsumer({
        'bootstrap.servers': process.env.KAFKA_URI,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': process.env.KAFKA_KEY,
        'sasl.password': process.env.KAFKA_SECRET,
        'group.id': process.env.KAFKA_CONSUMER_GROUP
    }, {
        'auto.offset.reset': 'earliest'
    });

    return new Promise((resolve, reject) => {
        consumer
            .on('ready', () => resolve(consumer))
            .on('data', onData);

        consumer.connect();
    });
}

(async () => {
    const consumer = await createConsumer(({ key, value, partition, offset }) => {
        console.log(`Message at offset ${offset}.`);
        console.log(`Consumed record ${value} of partition ${partition}.`);
        console.log();
    });

    consumer.subscribe([process.env.KAFKA_TOPIC]);
    consumer.consume();
})();
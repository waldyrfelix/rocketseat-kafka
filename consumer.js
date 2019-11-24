const Kafka = require('node-rdkafka');

function createConsumer(onData) {
    const consumer = new Kafka.KafkaConsumer({
        'bootstrap.servers': 'pkc-41973.westus2.azure.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '5X3WCHM645QMDBLS',
        'sasl.password': 'JFr40oqZpOfGWNYZm9f1sGj8rU/YHDXF+Vws3xYhIjrIV5QLkL6Rq2aj8IC7Vtbq',
        'group.id': 'rocketseat-consumer-group'
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
        console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}.`);
    });

    consumer.subscribe(['rocketseat-topic']);
    consumer.consume();

})();
const Kafka = require('node-rdkafka');
require("dotenv").config()

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function createProducer(onDeliveryReport) {
    const producer = new Kafka.Producer({
        'bootstrap.servers': process.env.KAFKA_URI,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': process.env.KAFKA_KEY,
        'sasl.password': process.env.kAFKA_SECRET,
        'dr_msg_cb': true
    });

    producer.setPollInterval(100);


    return new Promise((resolve, reject) => {
        producer
            .on('ready', () => {
                console.log("producer ready");
                resolve(producer);
            })
            .on('delivery-report', onDeliveryReport)
            .on('event.error', (err) => {
                console.warn('event.error', err);
                reject(err);
            });
        producer.connect();
    });
}

(async () => {
    const producer = await createProducer((err, report) => {
        if (err) {
            console.error(err);
        } else {
            const { topic, partition, value } = report;
            console.log(`Successfully produced record to topic "${topic}" partition ${partition} ${value}`);
        }
    });


    for (let i = 0; i < 10; i++) {
        let data = {
            index: i,
            name: "waldyr",
            title: "rocketseat",
            body: "Kafka rocks!"
        }

        const messageValue = Buffer.from(JSON.stringify(data))
        producer.produce(process.env.KAFKA_TOPIC, 0, messageValue)

        await sleep(1000)
    }

})();
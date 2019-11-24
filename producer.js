const Kafka = require('node-rdkafka');
/**
 * key: 5X3WCHM645QMDBLS
 * secret: JFr40oqZpOfGWNYZm9f1sGj8rU/YHDXF+Vws3xYhIjrIV5QLkL6Rq2aj8IC7Vtbq
 */

function createProducer(onDeliveryReport) {
    const producer = new Kafka.Producer({
        'bootstrap.servers': 'pkc-41973.westus2.azure.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '5X3WCHM645QMDBLS',
        'sasl.password': 'JFr40oqZpOfGWNYZm9f1sGj8rU/YHDXF+Vws3xYhIjrIV5QLkL6Rq2aj8IC7Vtbq',
        'dr_msg_cb': true
    });

    console.log("producer");


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
        console.log("producer report");
        
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
        producer.produce("rocketseat-topic", 0, messageValue)
    }

})();
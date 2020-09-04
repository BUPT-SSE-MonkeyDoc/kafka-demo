const {Kafka} = require('kafkajs');
const serverAddress = '121.36.15.90:9092'
const kafka = new Kafka({
    clientId:'MonkeyDoc',
    brokers:[serverAddress]
})

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group'});

    
    // Producing
    producer.connect()
    for(var i = 0 ; i < 100; i++)
    {
        producer.send({
            topic: 'test',
            messages: [
              { key:i.toString(), value: 'Hello KafkaJS user!' + i},
            ],
          })
    }

   
    // // Consuming
    // consumer.connect()
    // consumer.subscribe({ topic: 'test', fromBeginning: true})
   
    // consumer.run({
    //   eachMessage: async ({ topic, partition, message }) => {
    //     console.log({
    //       partition,
    //       offset: message.offset,
    //       value: message.value.toString(),
    //     })
    //   },
    // })

// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'my-app',
//   brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094'],
//   ssl: true,
//   sasl: {
//     mechanism: 'plain', // can be plain, scram-sha-256, scram-sha-512
//     username: 'kafka',
//     password: 'kafka-sasl-password'
//   }
// });

// const producer = kafka.producer();

// async function sendMessageToKafka(senderIdChat, receiverIdChat, content) {
//   await producer.connect();
//   await producer.send({
//     topic: 'chat-messages',
//     messages: [
//       { value: JSON.stringify({
//           firstUserId: senderIdChat,
//           secondUserId: receiverIdChat,
//           content: content,
//           date: Date.now()
//         }) 
//       },
//     ],
//   });
//   await producer.disconnect();
// }

// // Use this function where you need to send chat messages






const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
// const ChatMessages = require('./models/ChatMessages'); // Ensure the path is correct

mongoose.connect('mongodb://localhost/testmongo', {useNewUrlParser: true, useUnifiedTopology: true})
    .then(() => console.log('Connected to MongoDB...'))
    .catch(err => console.error('Could not connect to MongoDB...', err));

    const chatMessagesSchema = new mongoose.Schema({
        firstUserId: String,
        secondUserId: String,
        content: String,
        data: Date
    });

    const ChatMessages = mongoose.model('ChatMessage', chatMessagesSchema);
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094'],
  ssl: null,
  sasl: null
});

const consumer = kafka.consumer({ groupId: 'chat-group' });

async function runConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'chat-messages', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const messageData = JSON.parse(message.value.toString());
      console.log("------------------------------");
      console.log(messageData);
      console.log("------------------------------");
      const chatMessage = new ChatMessages({
        firstUserId: messageData.firstUserId,
        secondUserId: messageData.secondUserId,
        content: messageData.content,
        date: messageData.date
      });
      await chatMessage.save();
    },
  });
}

runConsumer().catch(console.error);

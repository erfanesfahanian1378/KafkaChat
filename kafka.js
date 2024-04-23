const {Kafka} = require('kafkajs');
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

const consumer = kafka.consumer({groupId: 'chat-group'});

async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({topic: 'chat-messages', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
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

require('dotenv').config();
const mongoose = require('mongoose');

const kafkaMessageSchema = new mongoose.Schema(
  {
    topic:     { type: String, required: true },
    partition: { type: Number, required: true },
    offset:    { type: String, required: true },
    key:       { type: String },
    payload:   { type: mongoose.Schema.Types.Mixed, required: true },
  },
  { timestamps: true }
);

const KafkaMessage = mongoose.model('KafkaMessage', kafkaMessageSchema);

const connectDb = async () => {
  const uri = process.env.MONGO_URI || 'mongodb://localhost:27017/kafka_db';
  await mongoose.connect(uri);
  console.log('MongoDB connecté:', uri);
};

module.exports = { KafkaMessage, connectDb };

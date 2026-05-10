require('dotenv').config();
const express = require('express');
const { KafkaMessage, connectDb } = require('./db');

const app = express();
app.use(express.json());

// GET /messages — retourne tous les messages (les 100 plus récents)
app.get('/messages', async (req, res) => {
  try {
    const messages = await KafkaMessage.find().sort({ createdAt: -1 }).limit(100);
    res.json(messages);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /messages/:id — retourne un message par son _id MongoDB
app.get('/messages/:id', async (req, res) => {
  try {
    const message = await KafkaMessage.findById(req.params.id);
    if (!message) return res.status(404).json({ error: 'Message non trouvé' });
    res.json(message);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

const PORT = process.env.API_PORT || 3000;

connectDb().then(() => {
  app.listen(PORT, () => {
    console.log(`API démarrée sur http://localhost:${PORT}`);
    console.log(`  GET http://localhost:${PORT}/messages`);
    console.log(`  GET http://localhost:${PORT}/messages/:id`);
  });
});

const { Kafka, CompressionTypes, Partitioners } = require('kafkajs');

const dataConn = {
  kafka_brokerconnect: process.env.KAFKA_BROKERCONNECT,
}

const kafka = new Kafka({
  clientId: 'loja-simulada',
  brokers: [dataConn.kafka_brokerconnect],
});

async function Publicar(msgs = []) {
    const mensagens = [];
    msgs.map((el) => mensagens.push({value: el}));
    const producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
      transactionalId: 'loja-simulada',
      maxInFlightRequests: 1,
      idempotent: true
    });
    await producer.connect();
    await producer.send({
        topic: 'vendas-deshboard-bronze',
        acks: -1,
        timeout: 60000,
        compression: CompressionTypes.GZIP,
        messages: mensagens,
    });
    await producer.disconnect();
}

module.exports = { Publicar }
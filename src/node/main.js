var kafka = require('kafka-node'),
  Producer = kafka.Producer,
  client = new kafka.Client(),
  producer = new Producer(client);



producer.on('ready', function() {

  for (let i = 0; i < 1000; i++) {

    var payload = [{
      topic: 'KTest',
      messages: JSON.stringify({
        "test": i,
        "Cooc": "qwwqw"
      }),
      partition: i % 16
    }];

    producer.send(payload, function(err, data) {
      console.log(data);
    });
  }
});


producer.on('error', function(err) {
  console.error(err);
});

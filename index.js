var WebSocketServer = require('websocket').server;
var http = require('http');
var debug = true;
const conf = {
    kafkaUrl : 'localhost',
    kafkaPort : '2181',
    serverPort : 3901,
    kafkaTopics : [
        { topic: 'test', partition: 0 }
    ]
};
var server = http.createServer(function(request, response) {
    if (debug) { console.log((new Date()) + ' Received request for ' + request.url); }
    response.writeHead(404);
    response.end();
});

var Gateway = function(){
    var events = {};
    var connect = function(){
        var kafka = require('kafka-node'),
            Producer = kafka.Producer,
            Consumer = kafka.Consumer,
            client = new kafka.Client(conf.kafkaUrl + ':' + conf.kafkaPort + '/'),
            producer = new Producer(client);
        var consumer = new Consumer(
            client,
            conf.kafkaTopics,
            {autoCommit: false}
        );
        consumer.on('message', function (event) {
            event = {
                type: "database",
                channel: "auto-policies",// record codes - "auto-policies", "exam-results"
                data: {
                    id: "24jpywj1",
                    model: {
                        id: "1jk8wxjd",
                        name: "Auto Policies"
                    },
                    data: {
                        manufacturer: "BMW",
                        model: "3 Series",
                        year: 2016,
                        monthly_premium: 480,
                        start_date: "2017-02-17",
                        duration: 12,
                        policy_number: "3411-2975",
                        customer_id: "jordanbrooks@gmail.com"
                    }
                },
                op: "delete",
                diff: []
            };
            // TODO Filter the message object to confirm it is valid to propagate to
            // console.log(event.value);
            // construct the message
            console.log("Got kafka message "+JSON.stringify(event));
            var key = event.type+':'+event.data.model.id;
            var val = events[key];
            if (!val) return false;
            val.forEach(function(item) {
                item(event);
            });
        });
    }();
    return {
        register : function(eventStream, callback){
            if (events[eventStream]){
                events[eventStream].push(callback)
            } else {
                events[eventStream] = [
                    callback
                ]
            }
        }
    }
}();
/*var gateway = (function() {
    console.log('Starting Kafka.........');
    var kafka = require('kafka-node'),
        Producer = kafka.Producer,
        Consumer = kafka.Consumer,
        client = new kafka.Client(conf.kafkaUrl + ':' + conf.kafkaPort + '/'),
        producer = new Producer(client);
    producer.on('error', function (err) {
        console.log('An error occured while starting kafka ' + err);
    });
    var postToKafka = function (topic, message) {
        const payloads = [{
            topic: topic,
            messages: message, // multi messages should be a array, single message can be just a string or a KeyedMessage instance
            key: 'theKey' // only needed when using keyed partitioner
        }];
        producer.send(payloads, function (err, data) {
            // console.log(data);
        });
    };

    var consumer = new Consumer(
        client,
        conf.kafkaTopics,
        {autoCommit: false}
    );
    consumer.on('message', function (event) {
        // TODO Filter the message object to confirm it is valid to propagate to
        console.log(event.value);
        handlers[event.stream].each(function() {
            this.handleMessage(event);
        });
    });
    var handlers = {};
    var registerHandler = function(eventStream, callback) {
        handlers[eventStream].push(callback);
    };
})(); */

var wsServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: true,
    maxReceivedFrameSize: 64*1024*1024,   // 64MiB
    maxReceivedMessageSize: 64*1024*1024, // 64MiB
    fragmentOutgoingMessages: false,
    keepalive: false,
    disableNagleAlgorithm: false
});
wsServer.on('connect', function(connection) {
    if (debug) {
        console.log((new Date()) + ' Connection accepted');
    }
    var handleMessage = function (message) {
        // TODO Filter the message object to confirm it is valid to propagate to the client
        console.log(message);
        connection.sendUTF(message, sendCallback);
    };
    function sendCallback(err) {
        if (err) {
            console.error('send() error: ' + err);
            connection.drop();
            setTimeout(function() {
                process.exit(100);
            }, 100);
        }
    }
    connection.on('message', function(message) {
        if (message.type === 'utf8') {
            try {
                console.log(JSON.stringify(message.utf8Data));
                var _message = typeof message.utf8Data === 'object' ? message.utf8Data : JSON.parse(message.utf8Data);
                console.log("Got client message "+JSON.stringify(message));
                Gateway.register(_message.query.event+':'+_message.query.databaseCode, handleMessage);
            } catch (e){
                console.log(e);
            }
        }
        /*else if (message.type === 'binary') {
            if (debug) { console.log('Received Binary Message of ' + message.binaryData.length + ' bytes'); }
            connection.sendBytes(message.binaryData, sendCallback);
        }*/
     });
});

/*producer.on('ready', function () {
    console.log('kafka is ready');
    canPostMessages = true;
});*/

server.listen(conf.serverPort, function() {
    console.log((new Date()) + ' Server is listening on port ' + conf.serverPort);
});
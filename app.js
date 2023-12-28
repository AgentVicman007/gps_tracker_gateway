require('dotenv').config();
const EspTracker = require('esptracker');
const Gt06 = require('gt06');
const Gps103 = require('gps103');
const Mqtt = require('mqtt');
const net = require('net');
const fs = require('fs');
const { Pool } = require('pg');

// gps tracker vars
const espTrackerServerPort = process.env.ESPTRACKER_SERVER_PORT || 64458;
const gt06ServerPort = process.env.GT06_SERVER_PORT || 64459;
const gps103ServerPort = process.env.GPS103_SERVER_PORT || 64460;

// mqtt vars
const rootTopic = process.env.MQTT_ROOT_TOPIC || 'tracker';
const brokerUrl = process.env.MQTT_BROKER_URL || 'localhost';
const brokerPort = process.env.MQTT_BROKER_PORT || 1883; // Use the default MQTT port without SSL
const mqttProtocol = process.env.MQTT_BROKER_PROTO || 'mqtt';
const brokerUser = process.env.MQTT_BROKER_USER || 'user';
const brokerPasswd = process.env.MQTT_BROKER_PASSWD || 'passwd';

// postgres vars
const pgHost = process.env.PG_HOST || 'localhost';
const pgUser = process.env.PG_USER || 'postgres';
const pgPasswd = process.env.PG_PASSWD || 'Josiah1!';
const pgDb = process.env.PG_DB || 'django';

// connect to PostgreSQL
const db = new Pool({
    host: pgHost,
    user: pgUser,
    password: pgPasswd,
    database: pgDb,
});

db.on('error', function (err) {
    console.log('postgresql:', err.code, err.fatal);
});

var mqttClient = Mqtt.connect({
    host: brokerUrl,
    port: brokerPort,
    protocol: mqttProtocol,
    username: brokerUser,
    password: brokerPasswd,
});

mqttClient.on('error', (err) => {
    console.error('MQTT Error:', err);
});

var espTrackerServer = net.createServer((client) => {
    var espT = new EspTracker();
    console.log(new Date().toISOString(), 'esp client connected', client.remoteAddress);

    espTrackerServer.on('error', (err) => {
        console.error('espTrackerServer error', err);
    });

    client.on('error', (err) => {
        console.error('esp client error', client.remoteAddress, err);
    });

    client.on('close', () => {
        console.log(new Date().toISOString(), 'esp client disconnected', client.remoteAddress);
    });

    client.on('data', (data) => {
        let msg = {};
        try {
            msg = espT.parse(data.toString());
        } catch (e) {
            console.log('esp err', client.remoteAddress, e);
            return;
        }
        msg.fixTimestamp = msg.fixTime.getTime() / 1000;

        mqttClient.publish(rootTopic + '/' + msg.imei + '/pos', JSON.stringify(msg));

        write2postgres(msg);
    });
});

var gt06Server = net.createServer((client) => {
    var gt06 = new Gt06();
    console.log(new Date().toISOString(), 'gt06 client connected', client.remoteAddress);

    gt06Server.on('error', (err) => {
        console.error('gt06Server error', err);
    });

    client.on('error', (err) => {
        console.error('gt06 client error', client.remoteAddress, err);
    });

    client.on('close', () => {
        console.log(new Date().toISOString(), 'gt06 client disconnected', client.remoteAddress);
    });

    client.on('data', (data) => {
        try {
            gt06.parse(data);
        } catch (e) {
            console.log('gt06 err', client.remoteAddress, e);
            return;
        }

        if (gt06.expectsResponse) {
            client.write(gt06.responseMsg);
        }
        gt06.msgBuffer.forEach((msg) => {
            let fixDatetime = new Date(msg.fixTime);
            if (fixDatetime < msg.parseTime - 3600000) {
                console.log('gt06 Invalid Position!');
            } else {
                mqttClient.publish(rootTopic + '/' + gt06.imei + '/pos', JSON.stringify(msg));

                if (msg.event.string === 'location') {
                    write2postgres(msg);
                }
            }
        });
        gt06.clearMsgBuffer();
    });
});

var gps103Server = net.createServer((client) => {
    var gps103 = new Gps103();
    console.log(new Date().toISOString(), 'gps103 client connected', client.remoteAddress);

    gps103Server.on('error', (err) => {
        console.error('gps103Server error', err);
    });

    client.on('error', (err) => {
        console.error('gps103 client error', client.remoteAddress, err);
    });

    client.on('close', () => {
        console.log(new Date().toISOString(), 'gps103 client disconnected', client.remoteAddress);
    });

    client.on('data', (data) => {
        try {
            gps103.parse(data);
        } catch (e) {
            console.log('gps103 err', client.remoteAddress, e);
            return;
        }

        if (gps103.expectsResponse) {
            client.write(gps103.responseMsg);
        }
        gps103.msgBuffer.forEach((msg) => {
            if (msg.hasFix) {
                mqttClient.publish(rootTopic + '/' + gps103.imei + '/pos', JSON.stringify(msg));
            }
            if (msg.hasFix && msg.event.string === 'location') {
                write2postgres(msg);
            }
        });
        gps103.clearMsgBuffer();
    });
});

espTrackerServer.listen(espTrackerServerPort, () => {
    console.log(new Date().toISOString(), 'started EspTracker server on port:', espTrackerServerPort);
});

gt06Server.listen(gt06ServerPort, () => {
    console.log(new Date().toISOString(), 'started GT06 server on port:', gt06ServerPort);
});

gps103Server.listen(gps103ServerPort, () => {
    console.log(new Date().toISOString(), 'started GPS103 server on port:', gps103ServerPort);
});

function write2postgres(posMsg) {
    let sql = `INSERT INTO positions(
        deviceId, position, latitude, longitude, fixTime, speed, heading
    ) VALUES (
        ${posMsg.imei}, ST_GeomFromText('POINT(${posMsg.lon} ${posMsg.lat})', 4326),
        ${posMsg.lat}, ${posMsg.lon}, TO_TIMESTAMP(${posMsg.fixTimestamp}),
        ${posMsg.speed}, ${posMsg.course}
    )`;

    db.query(sql, (err, results) => {
        if (err) console.error('postgresql error:', err);
    });
}

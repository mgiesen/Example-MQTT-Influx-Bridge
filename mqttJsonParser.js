const mqtt = require("mqtt");

let mqttJsonParser_client;
let mqttJsonParser_logger;

module.exports = {

    start: (options) =>
    {
        const { credentials, subscriptions, logger } = options;

        mqttJsonParser_logger = logger;
        mqttJsonParser_client = mqtt.connect(credentials);

        mqttJsonParser_client.on("close", () =>
        {
            mqttJsonParser_logger("MQTT connection closed");
            module.exports.stop();
        });

        mqttJsonParser_client.on("offline", () =>
        {
            mqttJsonParser_logger("MQTT broker connection failed");
            module.exports.stop();
        });

        mqttJsonParser_client.on("error", (error) =>
        {
            mqttJsonParser_logger(error);
            module.exports.stop();
        });

        mqttJsonParser_client.on('connect', () =>
        {
            mqttJsonParser_logger("MQTT connection successfully");

            subscriptions.forEach((sub) =>
            {
                if (sub.enabled)
                {
                    mqttJsonParser_client.subscribe(sub.topic);
                }
            });
        });

        mqttJsonParser_client.on('message', (topic, message) =>
        {
            const parsedMessage = JSON.parse(message.toString());

            subscriptions.forEach((sub) =>
            {
                if (sub.topic === topic)
                {
                    sub.mappings.forEach((mapping) =>
                    {
                        const keys = mapping.schema.split(".");
                        let value = parsedMessage;

                        keys.forEach((key) =>
                        {
                            if (value && value.hasOwnProperty(key))
                            {
                                value = value[key];
                            } else
                            {
                                value = null;
                            }
                        });

                        if (value !== null)
                        {
                            if (typeof mapping.onMessage === 'function')
                            {
                                if (mapping.hasOwnProperty('messageParameter'))
                                {
                                    mapping.onMessage(value, mapping.messageParameter);
                                }
                                else
                                {
                                    mapping.onMessage(value);
                                }
                            }
                        }
                    });
                }
            });
        });
    },
    stop: () => 
    {
        if (mqttJsonParser_client) 
        {
            mqttJsonParser_client.end(true, () =>
            {
                if (mqttJsonParser_logger)
                {
                    mqttJsonParser_logger("MQTT broker connection is closed now");
                }
                mqttJsonParser_client = null;
            });
        }
    },
    isRunning: () => 
    {
        return mqttJsonParser_client !== null;
    }

};

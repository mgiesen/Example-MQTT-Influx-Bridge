const Influx = require("influx");
const mqttJsonParser = require("./mqttJsonParser.js");

require('dotenv').config();

const INFORMATIONAL_LOGS = true;

// The config object for MQTT Broker
const mqtt_config = {
    host: process.env.MQTT_HOST,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    protocol: process.env.MQTT_PROTOCOL
};

// Logger for MQTT Broker
function mqttJsonParser_logger(message)
{
    console.log("[mqttJsonParser]", message);
}

// Initialization method for databases 
async function init_database(database_name, database_schema)
{
    const influx_config = {
        host: process.env.INFLUX_HOST,
        port: process.env.INFLUX_PORT,
        username: process.env.INFLUX_USERNAME,
        password: process.env.INFLUX_PASSWORD,
        database: database_name,
        schema: database_schema
    };

    const influx = new Influx.InfluxDB(influx_config);

    try
    {
        const names = await influx.getDatabaseNames();
        if (!names.includes(database_name))
        {
            await influx.createDatabase(database_name);
        }
        return influx;
    }
    catch (err)
    {
        console.error(err);
        return false;
    }
}

// Method for adding data to database
function add_to_database(value, messageParameter)
{
    let new_point = [{
        measurement: messageParameter.table,
        fields: {
            value: parseFloat(value),
        },
        tags: messageParameter.tags
    }];

    messageParameter.database.writePoints(new_point).catch(console.error);

    if (INFORMATIONAL_LOGS) 
    {
        console.log(`[Database] Added value ${value} to table ${messageParameter.table}`);
    }
}

// Initialize and start MQTT listener for specified DATABASE
function start_mqttJsonParser(DATABASE)
{
    mqttJsonParser.start({
        credentials: mqtt_config,
        logger: mqttJsonParser_logger,
        subscriptions: [
            {
                topic: "tele/tasmota-fridge/SENSOR",
                enabled: true,
                mappings: [
                    {
                        schema: "ENERGY.Power",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "fridge-power",
                            tags: {}
                        }
                    },
                    {
                        schema: "ENERGY.Today",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "fridge-energy-today",
                            tags: {}
                        }
                    }
                ]
            },
            {
                topic: "tele/tasmota-desk-m/SENSOR",
                enabled: true,
                mappings: [
                    {
                        schema: "ENERGY.Power",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "desk-m-power",
                            tags: {}
                        }
                    },
                    {
                        schema: "ENERGY.Today",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "desk-m-energy-today",
                            tags: {}
                        }
                    }
                ]
            },
            {
                topic: "tele/tasmota-desk-k/SENSOR",
                enabled: true,
                mappings: [
                    {
                        schema: "ENERGY.Power",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "desk-k-power",
                            tags: {}
                        }
                    },
                    {
                        schema: "ENERGY.Today",
                        onMessage: add_to_database,
                        messageParameter: {
                            database: DATABASE,
                            table: "desk-k-energy-today",
                            tags: {}
                        }
                    }
                ]
            }
        ]
    });
}

async function main()
{
    // Definition of database
    const DATABASE = await init_database("TASMOTA", [{
        measurement: "parameter",
        fields: {
            value: Influx.FieldType.FLOAT,
        },
        tags: ["topic"],
    }]);

    if (!DATABASE)
    {
        console.error("Database initialization failed");
        return;
    }

    // Initialize and start MQTT listener for specified DATABASE
    start_mqttJsonParser(DATABASE);

    // Monitoring the instance
    setInterval(() =>
    {
        if (!mqttJsonParser.isRunning())
        {
            console.error("mqttJsonParser failed. Restart instance...");
            start_mqttJsonParser(DATABASE);
        }
    }, 10000);

}

main().catch(console.error);

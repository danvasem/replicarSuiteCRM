const mysql = require("mysql");

exports.lambdaHandler = (event, context, callback) => {
  try {
    //context.callbackWaitsForEmptyEventLoop = false
    console.log("Evento recibido: " + JSON.stringify(event));
    let connection = mysql.createConnection({
      host: "vinco.cluster-c7ywgpxaofs3.us-east-1.rds.amazonaws.com",
      user: "dvs_admin",
      password: "dvsxamarin",
      port: 3306,
      database: event.database
    });
    connection.query(event.query, function(err, result) {
      if (err) {
        console.log("Error en la ejecución del query: " + err.message);
        callback(err);
      } else {
        connection.end();
        callback(null, result);
      }
    });
  } catch (ex) {
    console.log("Error en la ejecución de la migración: " + ex.message);
    callback(ex);
  }
};

/**
 *
 * Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 *
 * Context doc: https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-context.html
 * @param {Object} context
 *
 * Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 *
 */

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const { crearCliente, actualizarCliente, crearClienteNegocio, crearCalificacionNegocio } = require("./cliente");
const { registrarAcumulacion, registrarAfiliacion, registrarRedencion } = require("./eventos");
const { crearCuenta, actualizarCuenta } = require("./cuenta");
const { crearPartida, actualizarPartida } = require("./partida");
const { crearCodigoCliente, actualizarCodigoCliente } = require("./vincard");

const TIPO_EVENTO_ACUMULACION = 1;
const TIPO_EVENTO_AFILIACION = 3;
const TIPO_EVENTO_REDENCION = 2;

const TIPO_PAQUETE_SUITECRM = 2;
const TIPO_MENSAJE_REPLICAR_TODOS_PAQUETES = 1;

exports.lambdaHandler = async (event, context) => {
  console.log(JSON.stringify(event));
  const data = JSON.parse(event.Records[0].Sns.Message);
  console.log(JSON.stringify(data));
  try {
    let paquetesPendientes = [];
    if (data.TipoMensaje === TIPO_MENSAJE_REPLICAR_TODOS_PAQUETES) {
      //Obtenemos todos los paquetes indistintamente del cliente o engocio
      paquetesPendientes = await obtenerTodosPaquetesPendientesProcesar();
    } else {
      //Obtenemos solo los paquetes del cliente con el negocio actual
      paquetesPendientes = await obtenerPaquetesPendientesProcesar(data.IdCliente, data.IdNegocio);
    }
    //Ejecutamos el paquete actual. NOTA: si el mensaje es replicar todos los paquetes, entonces NO se ejecuta el actual
    if (data.TipoMensaje === 1 || (await ejecutarPaquete(data, event))) {
      //Considerar que solo si el paquete actual ejecuta correctamente se ejecutaran los demás paquetes
      if (paquetesPendientes.length) {
        console.log("Ejecución de paquetes pendiente por procesar.");
        console.log("Cantidad de paquetes: " + paquetesPendientes.length);
        for (let i = 0; i < paquetesPendientes.length; i++) {
          let paquete = paquetesPendientes[i];
          console.log(paquete.MsjPaqueteRDS.S);
          if (await ejecutarPaquete(JSON.parse(paquete.MsjPaqueteRDS.S))) {
            await eliminarPaquetePendienteProcesar(paquete);
          }
        }
      }
    }
    return true;
  } catch (err) {
    console.log("Error en la ejecución general de replicación: " + err.message);
  }
  return false;
};

/**
 * Ejecuta un único paquete, si se produce un error entonces se guarda en VC_PaqueteRDS
 * @param {object} data Objeto con los datos del paquete a ejecutar
 * @returns {bool} True si la ejecución fue correcta
 */
const ejecutarPaquete = async (data, event = null) => {
  try {
    for (let i = 0; i < data.ListaRegistros.length; i++) {
      let record = data.ListaRegistros[i];
      if (record.$type.includes("RDS.RdsClienteNegocio")) {
        switch (record.TipoOperacion) {
          case 1:
            await crearClienteNegocio(record);
          case 2: {
            if (record.Rating) await crearCalificacionNegocio(record);
          }
        }
      } else if (record.$type.includes("RDS.RdsCliente")) {
        switch (record.TipoOperacion) {
          case 1: {
            await crearCliente(record);
          }
          case 2: {
            await actualizarCliente(record);
          }
        }
      } else if (record.$type.includes("RDS.RdsEvento")) {
        //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
        const campania = data.ListaRegistros.find(X => X.$type.includes("RDS.RdsMovPartida") && X.IdCampania != null);
        const idCampania = campania ? campania.IdCampania.IdClaveForanea : null;
        switch (record.IdTipoEvento.IdClaveForanea) {
          case TIPO_EVENTO_ACUMULACION:
            await registrarAcumulacion(record, idCampania);
          case TIPO_EVENTO_AFILIACION:
            await registrarAfiliacion(record, idCampania);
        }
      } else if (record.$type.includes("RDS.RdsRedencion")) {
        //obtenemos el elemento del evento
        const evento = data.ListaRegistros.find(
          X => X.$type.includes("RDS.RdsEvento") && X.IdTipoEvento.IdClaveForanea === TIPO_EVENTO_REDENCION
        );
        const cuenta = data.ListaRegistros.find(
          X => X.$type.includes("RDS.RdsCuenta") && X.IdEntidad === record.IdCuenta.IdEntidad
        );
        await registrarRedencion(record, evento, cuenta);
      } else if (record.$type.includes("RDS.RdsCuenta")) {
        switch (record.TipoOperacion) {
          case 1:
            await crearCuenta(record);
          case 2:
            await actualizarCuenta(record);
        }
      } else if (record.$type.includes("RDS.RdsPartida")) {
        switch (record.TipoOperacion) {
          case 1: {
            const evento = data.ListaRegistros.find(X => X.$type.includes("RDS.RdsEvento"));
            //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
            const campania = data.ListaRegistros.find(
              X => X.$type.includes("RDS.RdsMovPartida") && X.IdCampania != null
            );
            const idCampania = campania ? campania.IdCampania.IdClaveForanea : null;
            await crearPartida(record, evento, idCampania);
          }
          case 2:
            await actualizarPartida(record);
        }
      } else if (record.$type.includes("RDS.RdsCodigoCliente")) {
        switch (record.TipoOperacion) {
          case 1:
            await crearCodigoCliente(record);
          case 2:
            await actualizarCodigoCliente(record);
        }
      }
    }
    return true;
  } catch (err) {
    console.log("Error en Ejecuar Paquete: " + err.message);
    if (event) await registarPaqueteTablaTemporal(event, data, err.message);
    return false; //Se indica que el paquete dió error y se almacenó en VP_PaqueteRDS
  }
};

/**
 * Guarda el evento temporalmente en la tabla VC_PaqueteRDS
 * @param {string} event El evento SNS tal como fue recibido por el función lambda.
 * @param {string} data La data del mensaje ya convertido en objeto JSON
 * @param {string} err El mensaje de error
 */
const registarPaqueteTablaTemporal = async (event, data, err) => {
  console.log("Procedemos a guardar temporalmente el registro en la tabla VC_PaqueteRDS");
  try {
    const item = {
      TableName: "VC_PaqueteRDS",
      Item: {
        IdCliente: { N: data.IdCliente.toString() },
        IdNegocio: { N: data.IdNegocio.toString() },
        NegocioFecha: {
          S: `${data.IdNegocio}-${TIPO_PAQUETE_SUITECRM}-${1000000 + Math.abs(Math.floor(Math.random() * 10000000))}`
        },
        MsjPaqueteRDS: { S: event.Records[0].Sns.Message },
        FechaMsj: { S: data.Fecha },
        FechaUlt: { S: new Date().toISOString() },
        CodError: { N: "-1" },
        MsjError: { S: err }
      }
    };
    await dynamodb.putItem(item).promise();
    console.log("Registro guardado.");
  } catch (ex) {
    console.log("Error al guardar temporalmente el registro en la tabla VC_PaqueteRDS: " + ex.message);
  }
};

/**
 * Obtiene los paquetes que están registrados en VC_PaqueteRDS. Estos paquetes fueron almacenados debido a que
 * en su ultima ejecución provocaron error.
 * @param {int} idCliente
 * @param {int} idNegocio
 * @returns {array} Lista de paquetes, en caso de no haber paquetes la lista es vacía.
 */
const obtenerPaquetesPendientesProcesar = async (idCliente, idNegocio) => {
  try {
    const params = {
      Select: "ALL_ATTRIBUTES",
      TableName: "VC_PaqueteRDS",
      ConsistentRead: true,
      ExpressionAttributeValues: {
        ":id": {
          N: idCliente.toString()
        },
        ":sortValue": {
          S: `${idNegocio}-${TIPO_PAQUETE_SUITECRM}-`
        }
      },
      KeyConditionExpression: "IdCliente=:id AND begins_with(NegocioFecha,:sortValue)"
    };

    const res = await dynamodb.query(params).promise();

    return res.Items;
  } catch (ex) {
    console.log("Error en obtener el paquete pendiente de procesar: " + ex);
    throw ex;
  }
};

const obtenerTodosPaquetesPendientesProcesar = async () => {
  try {
    const params = {
      TableName: "VC_PaqueteRDS",
      Select: "ALL_ATTRIBUTES"
    };
    const res = await dynamodb.scan(params).promise();
    return res.Items;
  } catch (ex) {
    console.log("Error en obtener todos los paquetes pendientes de procesar: " + ex);
    throw ex;
  }
};

/**
 * Elimina un registro de la tabla VC_PaqueteRDS
 * @param {object} paquete Item de la tabla VC_PaqueteRDS que se debe eliminar
 */
const eliminarPaquetePendienteProcesar = async paquete => {
  try {
    //Eliminamos el paquete
    const params = {
      Key: {
        IdCliente: {
          N: paquete.IdCliente.N
        },
        NegocioFecha: {
          S: paquete.NegocioFecha.S
        }
      },
      TableName: "VC_PaqueteRDS"
    };
    await dynamodb.deleteItem(params).promise();
  } catch (ex) {
    console.log("Error al eliminar el paquete de la tabla VC_PaqueteRDS: " + ex.message);
    throw ex;
  }
};

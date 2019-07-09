const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const { crearCliente, actualizarCliente, crearClienteNegocio, crearCalificacionNegocio } = require("./cliente");
const {
  registrarAcumulacion,
  registrarAfiliacion,
  registrarRedencion,
  eliminarEvento,
  registrarReverso,
  registrarCuponJuego
} = require("./eventos");
const { crearCuenta, actualizarCuenta } = require("./cuenta");
const { crearPartida, actualizarPartida, eliminarPartida } = require("./partida");
const { crearCodigoCliente, actualizarCodigoCliente } = require("./vincard");
const { registrarLogNotificacionCliente } = require("./logNotificacionCliente");

const TIPO_EVENTO_ACUMULACION = 1;
const TIPO_EVENTO_AFILIACION = 3;
const TIPO_EVENTO_REDENCION = 2;
const TIPO_EVENTO_REVERSO = 4;
const TIPO_EVENTO_CUPON_JUEGO = 5;

const TIPO_OPERACION_INSERT = 1;
const TIPO_OPERACION_UPDATE = 2;
const TIPO_OPERACION_DELETE = 3;
const TIPO_OPERACION_NINGUNO = 4;

const TIPO_PAQUETE_SUITECRM = 2;
const TIPO_MENSAJE_REPLICAR_TODOS_PAQUETES = 1;

/**
 * Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 *
 * Context doc: https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-context.html
 * @param {Object} context
 *
 * Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 */

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
      let resultado = true;
      if (paquetesPendientes.length) {
        console.log("Ejecución de paquetes pendiente por procesar.");
        console.log("Cantidad de paquetes: " + paquetesPendientes.length);
        for (let i = 0; i < paquetesPendientes.length; i++) {
          let paquete = paquetesPendientes[i];
          console.log(paquete.MsjPaqueteRDS.S);
          if (await ejecutarPaquete(JSON.parse(paquete.MsjPaqueteRDS.S))) {
            await eliminarPaquetePendienteProcesar(paquete);
          } else {
            resultado = false; //Si al menos un paquete falla, entonces el resultado es falso.
          }
        }
      }
      return resultado;
    }
  } catch (err) {
    console.log("Error en la ejecución general de replicación: " + err.message);
  }
  return false; //Para indicar que el paquete ha presentado problemas en su ejecución
};

/**
 * Ejecuta un único paquete, si se produce un error entonces se guarda en VC_PaqueteRDS
 * @param {object} data Objeto con los datos del paquete a ejecutar
 * @returns {bool} True si la ejecución fue correcta
 */
const ejecutarPaquete = async (data, event = null) => {
  let contadorPaquete = 0;
  try {
    prepararPaquete(data.ListaRegistros);
    for (contadorPaquete = 0; contadorPaquete < data.ListaRegistros.length; contadorPaquete++) {
      let record = data.ListaRegistros[contadorPaquete];
      if (record.$type.includes("RDS.RdsClienteNegocio")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            await crearClienteNegocio(record);
            break;
          case TIPO_OPERACION_UPDATE:
            {
              if (record.Rating) await crearCalificacionNegocio(record);
            }
            break;
        }
      } else if (record.$type.includes("RDS.RdsCliente")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            {
              await crearCliente(record);
            }
            break;
          case TIPO_OPERACION_UPDATE:
            {
              await actualizarCliente(record);
            }
            break;
        }
      } else if (record.$type.includes("RDS.RdsEvento")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            {
              //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
              const campania = data.ListaRegistros.find(
                X => X.$type.includes("RDS.RdsMovPartida") && X.IdCampania != null
              );
              const idCampania = campania ? campania.IdCampania.IdClaveForanea : null;
              switch (record.IdTipoEvento.IdClaveForanea) {
                case TIPO_EVENTO_ACUMULACION:
                  await registrarAcumulacion(record, idCampania);
                  break;
                case TIPO_EVENTO_AFILIACION:
                  await registrarAfiliacion(record, idCampania);
                  break;
                case TIPO_EVENTO_REVERSO:
                  {
                    //Obtenemos el evento reversado
                    const evento = data.ListaRegistros.find(
                      X => X.$type.includes("RDS.RdsEvento") && X.IdTipoEvento.IdClaveForanea !== TIPO_EVENTO_REVERSO
                    );
                    await registrarReverso(record, evento);
                  }
                  break;
                case TIPO_EVENTO_CUPON_JUEGO:
                  {
                    await registrarCuponJuego(record);
                  }
                  break;
              }
            }
            break;
          case TIPO_OPERACION_DELETE:
            {
              await eliminarEvento(record);
            }
            break;
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
          case TIPO_OPERACION_INSERT:
            await crearCuenta(record);
            break;
          case TIPO_OPERACION_UPDATE:
            await actualizarCuenta(record);
            break;
        }
      } else if (record.$type.includes("RDS.RdsPartida")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            {
              //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
              const movPartida = data.ListaRegistros.find(
                X =>
                  X.$type.includes("RDS.RdsMovPartida") &&
                  X.IdPartida != null &&
                  X.IdPartida.IdEntidad === record.IdEntidad
              );
              await crearPartida(record, movPartida);
            }
            break;
          case TIPO_OPERACION_UPDATE:
            await actualizarPartida(record);
            break;
          case TIPO_OPERACION_DELETE:
            await eliminarPartida(record.NumeroUnico);
            break;
        }
      } else if (record.$type.includes("RDS.RdsCodigoCliente")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            await crearCodigoCliente(record);
            break;
          case TIPO_OPERACION_UPDATE:
            await actualizarCodigoCliente(record);
            break;
        }
      } else if (record.$type.includes("RDS.RdsLogNotificacionCliente")) {
        switch (record.TipoOperacion) {
          case TIPO_OPERACION_INSERT:
            await registrarLogNotificacionCliente(record);
            break;
        }
      }
    }
    return true;
  } catch (err) {
    console.log("Error en Ejecutar Paquete: " + err.message);
    if (event) await registarPaqueteTablaTemporal(data, contadorPaquete, err.message);
    return false; //Se indica que el paquete dió error y se almacenó en VP_PaqueteRDS
  }
};

/**
 * Guarda el evento temporalmente en la tabla VC_PaqueteRDS
 * @param {string} event El evento SNS tal como fue recibido por el función lambda.
 * @param {string} data La data del mensaje ya convertido en objeto JSON
 * @param {string} err El mensaje de error
 */
const registarPaqueteTablaTemporal = async (data, cantidadRegistrosEjecutados, err) => {
  console.log("Procedemos a guardar temporalmente el registro en la tabla VC_PaqueteRDS");
  console.log("Total de registros del paquete: " + data.ListaRegistros.length);
  console.log("Total de registros ejecutados: " + cantidadRegistrosEjecutados);
  console.log("Total de registros guardar: " + (data.ListaRegistros.length - cantidadRegistrosEjecutados));
  try {
    //Quitamos los registros que ya se han ejecutado
    data.ListaRegistros = data.ListaRegistros.slice(cantidadRegistrosEjecutados);

    const item = {
      TableName: "VC_PaqueteRDS",
      Item: {
        IdCliente: { N: data.IdCliente.toString() },
        IdNegocio: { N: data.IdNegocio.toString() },
        NegocioFecha: {
          S: `${data.IdNegocio}-${TIPO_PAQUETE_SUITECRM}-${1000000 + Math.abs(Math.floor(Math.random() * 10000000))}`
        },
        MsjPaqueteRDS: { S: JSON.stringify(data) },
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

    if (res.Items.length > 0) {
      //Ordenamos por la fecha del mensaje de manera asc
      res.Items.sort((a, b) => {
        return new Date(a.FechaMsj.S) - new Date(b.FechaMsj.S);
      });
    }
    return res.Items;
  } catch (ex) {
    console.log("Error en obtener el paquete pendiente de procesar: " + ex.message);
    throw ex;
  }
};

/**
 * Prepara el orden de la lista de paquetes para su correcta ejecución
 * @param {array} listaPaquetes
 */
const prepararPaquete = listaPaquetes => {
  try {
    //Preguntamos por redención
    const redencion = listaPaquetes.find(X => X.$type.includes("RDS.RdsRedencion"));
    if (redencion != null) {
      //Procedemos a colocar redencion como primer registro
      listaPaquetes.splice(listaPaquetes.indexOf(redencion), 1);
      listaPaquetes.unshift(redencion);
    }
  } catch (ex) {
    console.log("Error en prepararPaquete: " + ex.message);
    throw ex;
  }
};

/**
 * Lee todos los paquetes de la tabla VC_PaqueteRDS
 * @returns {array} Lista de paquetes
 */
const obtenerTodosPaquetesPendientesProcesar = async () => {
  try {
    const params = {
      TableName: "VC_PaqueteRDS",
      Select: "ALL_ATTRIBUTES"
    };
    const res = await dynamodb.scan(params).promise();
    if (res.Items.length > 0) {
      //Ordenamos por la fecha del mensaje de manera asc
      res.Items.sort((a, b) => {
        return new Date(a.FechaMsj.S) - new Date(b.FechaMsj.S);
      });
    }
    return res.Items;
  } catch (ex) {
    console.log("Error en obtener todos los paquetes pendientes de procesar: " + ex.message);
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

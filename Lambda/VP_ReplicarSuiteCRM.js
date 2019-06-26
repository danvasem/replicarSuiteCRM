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

const { crearCliente, actualizarCliente, crearClienteNegocio, crearCalificacionNegocio } = require("./cliente");
const { registrarAcumulacion, registrarAfiliacion, registrarRedencion } = require("./eventos");
const { crearCuenta, actualizarCuenta } = require("./cuenta");
const { crearPartida, actualizarPartida } = require("./partida");

const TIPO_EVENTO_ACUMULACION = 1;
const TIPO_EVENTO_AFILIACION = 3;
const TIPO_EVENTO_REDENCION = 2;

exports.lambdaHandler = async (event, context) => {
  let res = "";
  try {
    const data = JSON.parse(event.Records[0].Sns.Message);
    console.log("Mensaje: " + data);

    /*******************ARTIFICIO**************************/
    //Identificamos si se debe "eseprar" para evitar el problema de la afiliación + acumulación
    if (
      data.ListaRegistros.find(
        X => X.$type.includes("RDS.RdsEvento") && X.IdTipoEvento.IdClaveForanea === TIPO_EVENTO_AFILIACION
      )
    ) {
      await sleep(2000);
    }
    /**************************************************** */

    await Promise.all(
      data.ListaRegistros.map(async record => {
        if (record.$type.includes("RDS.RdsClienteNegocio")) {
          switch (record.TipoOperacion) {
            case 1:
              res = await crearClienteNegocio(record);
              break;
            case 2:
              {
                if (record.Rating) res = await crearCalificacionNegocio(record);
              }
              break;
          }
        } else if (record.$type.includes("RDS.RdsCliente")) {
          switch (record.TipoOperacion) {
            case 1:
              {
                res = await crearCliente(record);
              }
              break;
            case 2:
              {
                res = await actualizarCliente(record);
              }
              break;
          }
        } else if (record.$type.includes("RDS.RdsEvento")) {
          //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
          const campania = data.ListaRegistros.find(X => X.$type.includes("RDS.RdsMovPartida") && X.IdCampania != null);
          const idCampania = campania ? campania.IdCampania.IdClaveForanea : null;
          switch (record.IdTipoEvento.IdClaveForanea) {
            case TIPO_EVENTO_ACUMULACION:
              res = await registrarAcumulacion(record, idCampania);
              break;
            case TIPO_EVENTO_AFILIACION:
              res = await registrarAfiliacion(record, idCampania);
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
          res = await registrarRedencion(record, evento, cuenta);
        } else if (record.$type.includes("RDS.RdsCuenta")) {
          switch (record.TipoOperacion) {
            case 1:
              await crearCuenta(record);
              break;
            case 2:
              await actualizarCuenta(record);
              break;
          }
        } else if (record.$type.includes("RDS.RdsPartida")) {
          switch (record.TipoOperacion) {
            case 1:
              {
                const evento = data.ListaRegistros.find(X => X.$type.includes("RDS.RdsEvento"));
                //Obtenemos la campania relacionada, NOTA: Esto es una simplificación que evita multiples-campañas
                const campania = data.ListaRegistros.find(
                  X => X.$type.includes("RDS.RdsMovPartida") && X.IdCampania != null
                );
                const idCampania = campania ? campania.IdCampania.IdClaveForanea : null;
                await crearPartida(record, evento, idCampania);
              }
              break;
            case 2:
              await actualizarPartida(record);
              break;
          }
        }
      })
    );
    return res;
  } catch (err) {
    console.log("Error: " + err.message);
    return err;
  }
};

function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

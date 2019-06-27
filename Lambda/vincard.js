const {
  crearModulo,
  crearRelacion,
  obtenerIdCliente,
  obtenerIdLocal,
  obtenerIdNegocio,
  actualizarModulo,
  obtenerModulo,
  legacyCrearRelacion
} = require("./helper");
const { formatSuiteCRMDateTime } = require("./helper");

/**
 * Crea una nueva Vincard en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const crearCodigoCliente = async record => {
  try {
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocio(record.IdNegocio.IdClaveForanea);

    const postData = JSON.stringify({
      data: {
        type: "qtk_codigo_cliente",
        attributes: {
          //id_partida_c //Campo no llega
          name: record.Codigo,
          codigo_c: record.Codigo,
          fecha_creacion_c: formatSuiteCRMDateTime(record.FechaCreacion),
          estado_c: record.Estado
        }
      }
    });

    //Procedemos a crear la afiliacion
    const registro = await crearModulo({
      modulo: "qtk_codigo_cliente",
      postData
    });

    //Procedemos a crear la relación con el Negocio
    let response = await legacyCrearRelacion({
      modulo: "qtk_codigo_cliente",
      moduloId: registro.data.id,
      nombreRelacion: "qtk_negocio_qtk_codigo_cliente_1",
      relatedId: idNegocio
    });

    //Procedemos a crear la relación con el Local
    response = await legacyCrearRelacion({
      modulo: "qtk_codigo_cliente",
      moduloId: registro.data.id,
      nombreRelacion: "qtk_local_qtk_codigo_cliente_1",
      relatedId: idLocal
    });

    return response;
  } catch (ex) {
    console.log("Error en crear CodigoCliente: " + ex.message);
    throw ex;
  }
};

/**
 * Actualiza una Vincard en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const actualizarCodigoCliente = async record => {
  try {
    const idRegistro = (await obtenerModulo({
      modulo: "qtk_codigo_cliente",
      nomUnico: record.Codigo,
      campoNomUnico: "codigo_c"
    })).id;

    const postData = JSON.stringify({
      data: {
        type: "qtk_codigo_cliente",
        id: idRegistro,
        attributes: {
          //id_partida_c //Campo no llega
          fecha_activacion_c: formatSuiteCRMDateTime(record.FechaActivacion),
          estado_c: record.Estado
        }
      }
    });

    //Procedemos a actualizar el registro
    const registro = await actualizarModulo({
      modulo: "qtk_codigo_cliente",
      postData
    });

    if (record.Estado === "A" && record.FechaActivacion) {
      const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
      const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
      const idNegocio = await obtenerIdNegocio(record.IdNegocio.IdClaveForanea);

      //Procedemos a crear la relación con el Cliente
      response = await crearRelacion({
        moduloDer: "Contacts",
        moduloIzq: "qtk_codigo_cliente",
        idDer: idCliente,
        idIzq: idRegistro
      });

      //Procedemos a crear la relación con el Negocio de activación
      response = await legacyCrearRelacion({
        modulo: "qtk_codigo_cliente",
        moduloId: registro.data.id,
        nombreRelacion: "qtk_negocio_qtk_codigo_cliente_2",
        relatedId: idNegocio
      });

      //Procedemos a crear la relación con el Local de activación
      response = await legacyCrearRelacion({
        modulo: "qtk_codigo_cliente",
        moduloId: registro.data.id,
        nombreRelacion: "qtk_local_qtk_codigo_cliente_2",
        relatedId: idLocal
      });

      return response;
    }

    return registro;
  } catch (ex) {
    console.log("Error en actualizar CodigoCliente: " + ex.message);
    throw ex;
  }
};

module.exports = { crearCodigoCliente, actualizarCodigoCliente };

const {
  crearModulo,
  crearRelacion,
  obtenerIdCliente,
  obtenerIdLocal,
  obtenerIdNegocioIdLocal,
  obtenerModulo,
  actualizarModulo,
  eliminarModulo
} = require("./helper");
const { formatSuiteCRMDateTime } = require("./helper");

/**
 * Registra una nueva partida en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const crearPartida = async (record, evento, campania = null) => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(evento.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocioIdLocal(idLocal);
    let idCampania = null;
    if (campania != null) {
      idCampania = (await obtenerModulo({
        modulo: "qtk_campania",
        id: campania,
        campoId: "id_campania_c"
      })).id;
    }

    const postData = JSON.stringify({
      data: {
        type: "qtk_partida",
        attributes: {
          //id_partida_c //Campo no llega
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          qtk_campania_id_c: idCampania,
          qtk_negocio_id_c: idNegocio,
          valor_alcanzado_c: record.ValorAlcanzado,
          progreso_c: record.Progreso,
          valor_cliente_c: record.ValorCliente,
          fecha_creacion_c: formatSuiteCRMDateTime(record.FechaCreacion),
          fecha_fin_c: record.FechaFin ? formatSuiteCRMDateTime(record.FechaFin) : null,
          estado_c: record.Estado,
          repeticion_c: record.Repeticion
        }
      }
    });

    //Procedemos a crear la afiliacion
    const partida = await crearModulo({
      modulo: "qtk_partida",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_partida",
      idDer: idCliente,
      idIzq: partida.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en crear Partida: " + ex.message);
    throw ex;
  }
};

/**
 * Actualiza una partida en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const actualizarPartida = async record => {
  try {
    const idPartida = (await obtenerModulo({
      modulo: "qtk_partida",
      nomUnico: record.NumeroUnico,
      campoNomUnico: "numero_unico_c"
    })).id;
    const postData = JSON.stringify({
      data: {
        type: "qtk_partida",
        id: idPartida,
        attributes: {
          valor_alcanzado_c: record.ValorAlcanzado,
          progreso_c: record.Progreso,
          valor_cliente_c: record.ValorCliente,
          fecha_fin_c: record.FechaFin ? formatSuiteCRMDateTime(record.FechaFin) : null,
          estado_c: record.Estado,
          repeticion_c: record.Repeticion
        }
      }
    });

    //Procedemos a crear la acumulacion
    const partida = await actualizarModulo({
      modulo: "qtk_partida",
      postData
    });

    return partida;
  } catch (ex) {
    console.log("Error en registrar actualizar Partida: " + ex.message);
    throw ex;
  }
};

/**
 * Elimina un registro de Partida en SuiteCRM
 * @param {object} record Objeto a eliminar
 * @returns {string} Respuesta de SuiteCRM
 */
const eliminarPartida = async numeroPartida => {
  try {
    const idModulo = (await obtenerModulo({
      modulo: "qtk_partida",
      nomUnico: numeroPartida,
      campoNomUnico: "numero_unico_c"
    })).id;

    response = await eliminarModulo({ modulo: "qtk_partida", idModulo });

    return response;
  } catch (ex) {
    console.log(`Error al eliminar la Partida ${numeroPartida}: ${ex.message}`);
    throw ex;
  }
};

module.exports = { crearPartida, actualizarPartida, eliminarPartida };

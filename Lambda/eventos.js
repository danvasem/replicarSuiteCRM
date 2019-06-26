const {
  crearModulo,
  crearRelacion,
  obtenerIdCliente,
  obtenerIdLocal,
  obtenerIdNegocioIdLocal,
  obtenerIdNegocio,
  obtenerIdTipoEvento,
  obtenerModulo
} = require("./helper");
const { formatSuiteCRMDateTime } = require("./helper");

/**
 * Registra una nueva acumulación en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const registrarAcumulacion = async (record, campania = null) => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocioIdLocal(idLocal);
    const idTipoEvento = await obtenerIdTipoEvento(record.IdTipoEvento.IdClaveForanea);
    let idCampania = null;
    if (campania != null) {
      idCampania = (await obtenerModulo({
        modulo: "qtk_campania",
        id: campania,
        campoId: "id_campania_c"
      })).id;
    }

    let estrellasGanadas = 0,
      puntosGanados = 0;
    if (record.ValoresAcumulados.length > 0) {
      estrellasGanadas = record.ValoresAcumulados[0].SaldoCuenta;
      puntosGanados = record.ValoresAcumulados[0].AvancePartida;
    }

    const postData = JSON.stringify({
      data: {
        type: "qtk_acumulacion",
        attributes: {
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          qtk_tipo_evento_id_c: idTipoEvento,
          fecha_acumulacion_c: formatSuiteCRMDateTime(record.FechaCreacion),
          valor_c: record.Valor,
          //usuario_responsable
          estado_c: record.Estado,
          tipo_codigo_cliente_c: record.TipoCodigoCliente,
          codigo_cliente_c: record.CodigoCliente,
          qtk_campania_id_c: idCampania,
          puntos_ganados_c: puntosGanados,
          estrellas_ganados_c: estrellasGanadas
          //vincard
        }
      }
    });

    //Procedemos a crear la acumulacion
    const acumulacion = await crearModulo({
      modulo: "qtk_acumulacion",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_acumulacion",
      idDer: idCliente,
      idIzq: acumulacion.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_local",
      moduloIzq: "qtk_acumulacion",
      idDer: idLocal,
      idIzq: acumulacion.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_acumulacion",
      idDer: idNegocio,
      idIzq: acumulacion.data.id
    });

    //PENDIENTE: Vincard

    return response;
  } catch (ex) {
    console.log("Error en registrar Acumulación: " + ex.message);
    throw ex;
  }
};

/**
 * Registra una nueva afiliación en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const registrarAfiliacion = async (record, campania = null) => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocioIdLocal(idLocal);
    const idTipoEvento = await obtenerIdTipoEvento(record.IdTipoEvento.IdClaveForanea);
    let idCampania = null;
    if (campania != null) {
      idCampania = (await obtenerModulo({
        modulo: "qtk_campania",
        id: campania,
        campoId: "id_campania_c"
      })).id;
    }

    let estrellasGanadas = 0,
      puntosGanados = 0;
    if (record.ValoresAcumulados.length > 0) {
      estrellasGanadas = record.ValoresAcumulados[0].SaldoCuenta;
      puntosGanados = record.ValoresAcumulados[0].AvancePartida;
    }

    const postData = JSON.stringify({
      data: {
        type: "qtk_afiliacion",
        attributes: {
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          qtk_tipo_evento_id_c: idTipoEvento,
          fecha_afiliacion_c: formatSuiteCRMDateTime(record.FechaCreacion),
          valor_c: record.Valor,
          //usuario_responsable
          estado_c: record.Estado,
          tipo_codigo_cliente_c: record.TipoCodigoCliente,
          codigo_cliente_c: record.CodigoCliente,
          qtk_campania_id_c: idCampania,
          puntos_ganados_c: puntosGanados,
          estrellas_ganados_c: estrellasGanadas
          //vincard
        }
      }
    });

    //Procedemos a crear la afiliacion
    const afiliacion = await crearModulo({
      modulo: "qtk_afiliacion",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_afiliacion",
      idDer: idCliente,
      idIzq: afiliacion.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_local",
      moduloIzq: "qtk_afiliacion",
      idDer: idLocal,
      idIzq: afiliacion.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_afiliacion",
      idDer: idNegocio,
      idIzq: afiliacion.data.id
    });

    //PENDIENTE: Vincard

    return response;
  } catch (ex) {
    console.log("Error en afiliar Cliente: " + ex.message);
    throw ex;
  }
};

/**
 * Registra una nueva redención en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const registrarRedencion = async (record, evento, cuenta) => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocio(record.IdNegocio.IdClaveForanea);
    const idTipoEvento = await obtenerIdTipoEvento(evento.IdTipoEvento.IdClaveForanea);
    const idPremio = (await obtenerModulo({
      modulo: "qtk_premio",
      id: record.IdPremio.IdClaveForanea,
      campoId: "id_premio_c"
    })).id;
    const idCuenta = (await obtenerModulo({
      modulo: "qtk_cuenta",
      nomUnico: cuenta.NumeroUnico,
      campoNomUnico: "numero_unico_c"
    })).id;

    const postData = JSON.stringify({
      data: {
        type: "qtk_redencion",
        attributes: {
          name: evento.NumeroUnico,
          numero_unico_c: evento.NumeroUnico,
          qtk_tipo_evento_id_c: idTipoEvento,
          fecha_redencion_c: formatSuiteCRMDateTime(record.FechaRedencion),
          valor_c: record.Valor,
          //usuario_responsable
          estado_c: evento.Estado,
          tipo_codigo_cliente_c: evento.TipoCodigoCliente,
          monto_referencial_c: record.MontoReferncial,
          qtk_cuenta_id_c: idCuenta,
          codigo_cliente_c: evento.CodigoCliente
          //vincard
        }
      }
    });

    //Procedemos a crear la afiliacion
    const redencion = await crearModulo({
      modulo: "qtk_redencion",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_redencion",
      idDer: idCliente,
      idIzq: redencion.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_local",
      moduloIzq: "qtk_redencion",
      idDer: idLocal,
      idIzq: redencion.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_redencion",
      idDer: idNegocio,
      idIzq: redencion.data.id
    });

    //Procedemos a crear la relación con el Premio
    response = await crearRelacion({
      moduloDer: "qtk_premio",
      moduloIzq: "qtk_redencion",
      idDer: idPremio,
      idIzq: redencion.data.id
    });

    //PENDIENTE: Vincard

    return response;
  } catch (ex) {
    console.log("Error en registrar Redención: " + ex.message);
    throw ex;
  }
};

module.exports = { registrarAcumulacion, registrarAfiliacion, registrarRedencion };

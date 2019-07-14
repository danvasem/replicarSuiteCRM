const {
  crearModulo,
  crearRelacion,
  obtenerIdCliente,
  obtenerIdLocal,
  obtenerIdNegocioIdLocal,
  obtenerIdNegocio,
  obtenerIdTipoEvento,
  obtenerIdUsuario,
  obtenerModulo,
  obtenerIdCodigoCliente,
  eliminarModulo,
  obtenerRelacionesModulo
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
    const idUsuario = await obtenerIdUsuario(record.IdUsuarioResponsable.IdClaveForanea);
    let idCodigoCliente = null;
    if (record.TipoCodigoCliente === "C" && record.CodigoCliente) {
      idCodigoCliente = await obtenerIdCodigoCliente(record.CodigoCliente);
    }
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
          user_id_c: idUsuario,
          estado_c: record.Estado,
          tipo_codigo_cliente_c: record.TipoCodigoCliente,
          codigo_cliente_c: record.CodigoCliente,
          qtk_campania_id_c: idCampania,
          puntos_ganados_c: puntosGanados,
          estrellas_ganados_c: estrellasGanadas
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

    if (idCodigoCliente) {
      response = await crearRelacion({
        moduloDer: "qtk_codigo_cliente",
        moduloIzq: "qtk_acumulacion",
        idDer: idCodigoCliente,
        idIzq: acumulacion.data.id
      });
    }

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
    const idUsuario = await obtenerIdUsuario(record.IdUsuarioResponsable.IdClaveForanea);
    let idCodigoCliente = null;
    if (record.TipoCodigoCliente === "C" && record.CodigoCliente) {
      idCodigoCliente = await obtenerIdCodigoCliente(record.CodigoCliente);
    }
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
          user_id_c: idUsuario,
          estado_c: record.Estado,
          tipo_codigo_cliente_c: record.TipoCodigoCliente,
          codigo_cliente_c: record.CodigoCliente,
          qtk_campania_id_c: idCampania,
          puntos_ganados_c: puntosGanados,
          estrellas_ganados_c: estrellasGanadas
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

    if (idCodigoCliente) {
      response = await crearRelacion({
        moduloDer: "qtk_codigo_cliente",
        moduloIzq: "qtk_afiliacion",
        idDer: idCodigoCliente,
        idIzq: afiliacion.data.id
      });
    }

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
    const idUsuario = await obtenerIdUsuario(evento.IdUsuarioResponsable.IdClaveForanea);
    let idCodigoCliente = null;
    if (evento.TipoCodigoCliente === "C" && evento.CodigoCliente) {
      idCodigoCliente = await obtenerIdCodigoCliente(evento.CodigoCliente);
    }
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
          user_id_c: idUsuario,
          estado_c: evento.Estado,
          tipo_codigo_cliente_c: evento.TipoCodigoCliente,
          monto_referencial_c: record.MontoReferncial,
          qtk_cuenta_id_c: idCuenta,
          codigo_cliente_c: evento.CodigoCliente
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

    if (idCodigoCliente) {
      response = await crearRelacion({
        moduloDer: "qtk_codigo_cliente",
        moduloIzq: "qtk_redencion",
        idDer: idCodigoCliente,
        idIzq: redencion.data.id
      });
    }

    return response;
  } catch (ex) {
    console.log("Error en registrar Redención: " + ex.message);
    throw ex;
  }
};

/**
 * Elimina un registro de Evento en SuiteCRM
 * @param {object} record Objeto a eliminar
 * @returns {string} Respuesta de SuiteCRM
 */
const eliminarEvento = async record => {
  try {
    //Obtenemos le id del módulo
    let modulo = null;
    switch (record.IdTipoEvento.IdClaveForanea) {
      case 1:
        modulo = "qtk_acumulacion";
        break;
      case 2:
        modulo = "qtk_redencion";
        break;
      case 3:
        modulo = "qtk_afiliacion";
        break;
    }
    const idModulo = (await obtenerModulo({
      modulo: modulo,
      nomUnico: record.NumeroUnico,
      campoNomUnico: "numero_unico_c"
    })).id;

    response = await eliminarModulo({ modulo, idModulo });

    return response;
  } catch (ex) {
    console.log(`Error en eliminar evento ${modulo}: ${ex.message}`);
    throw ex;
  }
};

/**
 * Registra un evento de Reverso en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const registrarReverso = async (record, evento) => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocioIdLocal(idLocal);
    const idTipoEvento = await obtenerIdTipoEvento(record.IdTipoEvento.IdClaveForanea);
    const idTipoEventoReversado = await obtenerIdTipoEvento(evento.IdTipoEvento.IdClaveForanea);
    const idUsuario = await obtenerIdUsuario(record.IdUsuarioResponsable.IdClaveForanea);

    //Obtenemos el evento original
    let modulo = "";
    let idPremio = null;
    let nombreCampoFechaCreacion = null;
    switch (evento.IdTipoEvento.IdClaveForanea) {
      case 1:
        {
          modulo = "qtk_acumulacion";
          nombreCampoFechaCreacion = "fecha_acumulacion_c";
        }
        break;
      case 2:
        {
          modulo = "qtk_redencion";
          nombreCampoFechaCreacion = "fecha_redencion_c";
        }
        break;
      case 3:
        {
          modulo = "qtk_afiliacion";
          nombreCampoFechaCreacion = "fecha_afiliacion_c";
        }
        break;
      case 5:
        {
          modulo = "qtk_cupon_juego";
          nombreCampoFechaCreacion = "fecha_canje_cupon_c";
        }
        break;
    }
    const moduloEvento = await obtenerModulo({
      modulo,
      campos: [nombreCampoFechaCreacion, "valor_c"],
      nomUnico: evento.NumeroUnico,
      campoNomUnico: "numero_unico_c"
    });

    //En el caso de redención, debemon obtener el Id del premio
    if (evento.IdTipoEvento.IdClaveForanea === 2 && moduloEvento != null) {
      const premio = await obtenerRelacionesModulo({
        modulo: "qtk_redencion",
        id: moduloEvento.id,
        nombreRelacion: "qtk_premio_qtk_redencion_1"
      });
      idPremio = premio.data[0].id;
    } else if (record.IdPremioReversado != null) {
      idPremio = (await obtenerModulo({
        modulo: "qtk_premio",
        id: record.IdPremioReversado,
        campoId: "id_premio_c"
      })).id;
    }

    const postData = JSON.stringify({
      data: {
        type: "qtk_reverso",
        attributes: {
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          qtk_tipo_evento_id_c: idTipoEvento,
          fecha_reverso_c: formatSuiteCRMDateTime(record.FechaCreacion),
          user_id_c: idUsuario,
          estado_c: record.Estado,
          qtk_tipo_evento_id1_c: idTipoEventoReversado,
          reverso_valor_c: record.ValorReversado ? record.ValorReversado : moduloEvento.name_value_list.valor_c.value,
          reverso_fecha_c: record.FechaEventoReversado
            ? formatSuiteCRMDateTime(record.FechaEventoReversado)
            : formatSuiteCRMDateTime(moduloEvento.name_value_list[nombreCampoFechaCreacion].value),
          reverso_numero_c: evento.NumeroUnico,
          qtk_premio_id_c: idPremio
        }
      }
    });

    //Procedemos a crear la acumulacion
    const reverso = await crearModulo({
      modulo: "qtk_reverso",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_reverso",
      idDer: idCliente,
      idIzq: reverso.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_local",
      moduloIzq: "qtk_reverso",
      idDer: idLocal,
      idIzq: reverso.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_reverso",
      idDer: idNegocio,
      idIzq: reverso.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en registrar Reverso: " + ex.message);
    throw ex;
  }
};

/**
 * Registra un nuevo evento de canje de cupón en SuiteCRM
 * @param {object} record El objeto con los datos del cupón
 * @returns {object} Respuesta de SuiteCRM
 */
const registrarCuponJuego = async record => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idLocal = await obtenerIdLocal(record.IdLocal.IdClaveForanea);
    const idNegocio = await obtenerIdNegocioIdLocal(idLocal);
    const idTipoEvento = await obtenerIdTipoEvento(record.IdTipoEvento.IdClaveForanea);
    const idUsuario = await obtenerIdUsuario(record.IdUsuarioResponsable.IdClaveForanea);

    let estrellasGanadas = 0,
      puntosGanados = 0;
    if (record.ValoresAcumulados.length > 0) {
      estrellasGanadas = record.ValoresAcumulados[0].SaldoCuenta;
      puntosGanados = record.ValoresAcumulados[0].AvancePartida;
    }

    const postData = JSON.stringify({
      data: {
        type: "qtk_cupon_juego",
        attributes: {
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          qtk_tipo_evento_id_c: idTipoEvento,
          fecha_canje_cupon_c: formatSuiteCRMDateTime(record.FechaCreacion),
          valor_c: record.Valor,
          user_id_c: idUsuario,
          estado_c: record.Estado,
          tipo_codigo_cliente_c: record.TipoCodigoCliente,
          codigo_cliente_c: record.CodigoCliente,
          cupon_canjeado_c: record.CodigoCupon,
          puntos_ganados_c: puntosGanados,
          estrellas_ganados_c: estrellasGanadas
        }
      }
    });

    //Procedemos a crear el cupón juego
    const cupon = await crearModulo({
      modulo: "qtk_cupon_juego",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_cupon_juego",
      idDer: idCliente,
      idIzq: cupon.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_local",
      moduloIzq: "qtk_cupon_juego",
      idDer: idLocal,
      idIzq: cupon.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_cupon_juego",
      idDer: idNegocio,
      idIzq: cupon.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en registrar Cupón Juego: " + ex.message);
    throw ex;
  }
};

module.exports = {
  registrarAcumulacion,
  registrarAfiliacion,
  registrarRedencion,
  eliminarEvento,
  registrarReverso,
  registrarCuponJuego
};

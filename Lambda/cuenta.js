const {
  crearModulo,
  obtenerModulo,
  actualizarModulo,
  crearRelacion,
  obtenerIdCliente,
  obtenerIdNegocio
} = require("./helper");
const { formatSuiteCRMDateTime } = require("./helper");

/**
 * Crear una nueva cuenta en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const crearCuenta = async record => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idNegocio = await obtenerIdNegocio(record.IdNegocio.IdClaveForanea);

    const postData = JSON.stringify({
      data: {
        type: "qtk_cuenta",
        attributes: {
          //id_cuenta_c: //No llega este campo en la replicación
          name: record.NumeroUnico,
          numero_unico_c: record.NumeroUnico,
          saldo_disponible_c: record.SaldoDisponible,
          saldo_disponible_base_c: record.SaldoDisponibleBase,
          saldo_contable_c: record.SaldoContable,
          saldo_contable_base_c: record.SaldoContableBase,
          fecha_apertura_c: formatSuiteCRMDateTime(record.FechaApertura),
          fecha_vigencia_c: formatSuiteCRMDateTime(record.FechaVigencia),
          fecha_expiracion_c: formatSuiteCRMDateTime(record.FechaExpiracion),
          estado_c: record.Estado
        }
      }
    });

    //Procedemos a crear la acumulacion
    const cuenta = await crearModulo({
      modulo: "qtk_cuenta",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_cuenta",
      idDer: idCliente,
      idIzq: cuenta.data.id
    });

    //Procedemos a crear la relación con el Local
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_cuenta",
      idDer: idNegocio,
      idIzq: cuenta.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en crear Cuenta: " + ex.message);
    throw ex;
  }
};

/**
 * Actualiza los saldos de una cuenta en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const actualizarCuenta = async record => {
  try {
    const idCuenta = (await obtenerModulo({
      modulo: "qtk_cuenta",
      nomUnico: record.NumeroUnico,
      campoNomUnico: "numero_unico_c"
    })).id;
    const postData = JSON.stringify({
      data: {
        type: "qtk_cuenta",
        id: idCuenta,
        attributes: {
          saldo_disponible_c: record.SaldoDisponible,
          saldo_disponible_base_c: record.SaldoDisponibleBase,
          saldo_contable_c: record.SaldoContable,
          saldo_contable_base_c: record.SaldoContableBase
        }
      }
    });

    //Procedemos a crear la acumulacion
    const cuenta = await actualizarModulo({
      modulo: "qtk_cuenta",
      postData
    });

    return cuenta;
  } catch (ex) {
    console.log("Error en actualizar Cuenta: " + ex.message);
    throw ex;
  }
};

module.exports = { crearCuenta, actualizarCuenta };

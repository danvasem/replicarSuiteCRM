const { serviceCall } = require("./serviceCall");
const { crearModulo, obtenerModulo, obtenerIdCliente, obtenerIdNegocio, crearRelacion } = require("./helper");
const { formatSuiteCRMDateTime, formatSuiteCRMDate } = require("./helper");

/**
 * Crea un nuevo cliente en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const crearCliente = async record => {
  try {
    const postData = JSON.stringify({
      data: {
        type: "Contacts",
        attributes: {
          nombre_unico_c: record.NomUnicoCliente,
          id_cliente_c: record.IdRdsRegistro,
          first_name: record.Nombre ? record.Nombre : record.CorreoElectronico,
          last_name: record.Apellido,
          sexo_c: record.CodigoSexo,
          birthdate: formatSuiteCRMDate(record.FechaNacimiento),
          ciudad_c: record.CodigoCiudad,
          pais_c: record.CodigoPais,
          direccion_c: record.Direccion,
          phone_mobile: record.TelefonoMovil,
          email1: record.CorreoElectronico,
          fecha_creacion_vinco_c: formatSuiteCRMDateTime(record.FechaCreacion),
          fecha_actualizacion_vinco_c: formatSuiteCRMDateTime(record.FechaUltimaModificacion),
          fecha_registro_vinco_c: formatSuiteCRMDateTime(record.FechaRegistro),
          app_registro_vinco_c: record.AppRegistro ? record.AppRegistro.substring(0, 1) : null,
          tipo_login_c: record.TipoLogin,
          estado_vinco_c: record.Estado
        }
      }
    });
    //Procedemos a crear la acumulacion
    const response = await crearModulo({
      modulo: "Contacts",
      postData
    });
    return response;
  } catch (ex) {
    console.log("Error en crear Cliente: " + ex.message);
    throw ex;
  }
};

/**
 * Actualiza un cliente existente en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const actualizarCliente = async record => {
  try {
    //Primero obtenemos el Id SuiteCRM del cliente a actualizar
    const contactId = (await obtenerModulo({
      modulo: "Contacts",
      id: record.IdRdsRegistro,
      campoId: "id_cliente_c"
    })).id;

    //Realizamos la actualización del registro
    const postData = JSON.stringify({
      data: {
        type: "Contacts",
        id: contactId,
        attributes: {
          name: record.Nombre ? `${record.Nombre} ${record.Apellido}` : record.CorreoElectronico,
          first_name: record.Nombre,
          last_name: record.Apellido,
          sexo_c: record.CodigoSexo,
          birthdate: formatSuiteCRMDate(record.FechaNacimiento),
          ciudad_c: record.CodigoCiudad,
          pais_c: record.CodigoPais,
          direccion_c: record.Direccion,
          phone_mobile: record.TelefonoMovil,
          email1: record.CorreoElectronico,
          fecha_actualizacion_vinco_c: formatSuiteCRMDateTime(record.FechaUltimaModificacion)
        }
      }
    });
    const response = await serviceCall({
      path: "/Api/V8/module",
      postData,
      method: "PATCH"
    });
    return response;
  } catch (ex) {
    console.log("Error en actualizar Cliente: " + ex.message);
    throw ex;
  }
};

/**
 * Crea un registro Cliente Negocio en SuiteCRM
 * @param {string} record Datos del registro
 * @returns {string} Respuesta de SuiteCRM
 */
const crearClienteNegocio = async record => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const negocio = await obtenerModulo({
      modulo: "qtk_negocio",
      campos: ["name"],
      id: record.IdNegocio.IdClaveForanea,
      campoId: "id_negocio_c"
    });
    const idNegocio = negocio.id;
    const nombreNegocio = negocio.name_value_list["name"].value;

    const postData = JSON.stringify({
      data: {
        type: "qtk_cliente_negocio",
        attributes: {
          name: nombreNegocio,
          fecha_creacion_c: formatSuiteCRMDateTime(record.FechaCreacion)
        }
      }
    });

    //Procedemos a crear el registro
    const registro = await crearModulo({
      modulo: "qtk_cliente_negocio",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_cliente_negocio",
      idDer: idCliente,
      idIzq: registro.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_cliente_negocio",
      idDer: idNegocio,
      idIzq: registro.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en crear ClienteNegocio: " + ex.message);
    throw ex;
  }
};

const crearCalificacionNegocio = async record => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const negocio = await obtenerModulo({
      modulo: "qtk_negocio",
      campos: ["name"],
      id: record.IdNegocio.IdClaveForanea,
      campoId: "id_negocio_c"
    });
    const idNegocio = negocio.id;
    const nombreNegocio = negocio.name_value_list["name"].value;

    const postData = JSON.stringify({
      data: {
        type: "qtk_cliente_negocio_calificacion",
        attributes: {
          name: nombreNegocio,
          calificacion_c: record.Rating,
          fecha_calificacion_c: formatSuiteCRMDateTime(record.FechaCreacion)
        }
      }
    });

    //Procedemos a crear el registro
    const registro = await crearModulo({
      modulo: "qtk_cliente_negocio_calificacion",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_cliente_negocio_calificacion",
      idDer: idCliente,
      idIzq: registro.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_cliente_negocio_calificacion",
      idDer: idNegocio,
      idIzq: registro.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en crear CalificacionClienteNegocio: " + ex.message);
    throw ex;
  }
};

module.exports = { crearCliente, actualizarCliente, crearClienteNegocio, crearCalificacionNegocio };

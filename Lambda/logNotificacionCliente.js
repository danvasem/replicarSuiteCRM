const { crearModulo, crearRelacion, obtenerIdCliente, obtenerIdNegocio, formatSuiteCRMDateTime } = require("./helper");

const registrarLogNotificacionCliente = async record => {
  try {
    const idCliente = await obtenerIdCliente(record.IdCliente.IdClaveForanea);
    const idNegocio = await obtenerIdNegocio(record.IdNegocio.IdClaveForanea);

    //Si el titulo no tiene datos, se utiliza el nombre único del grupo
    const titulo = record.Titulo ? record.Titulo : record.NombreUnicoGrupo;

    const postData = JSON.stringify({
      data: {
        type: "qtk_log_notificacion_cliente",
        attributes: {
          name: titulo,
          fecha_notificacion_c: formatSuiteCRMDateTime(record.FechaEnvio),
          titulo_c: titulo,
          mensaje_c: record.Mensaje,
          nombre_unico_grupo_c: record.NombreUnicoGrupo,
          error_c: record.Error,
          canal_c: record.Canal,
          estado_c: record.Estado
        }
      }
    });

    //Procedemos a crear el registro
    const notificacion = await crearModulo({
      modulo: "qtk_log_notificacion_cliente",
      postData
    });

    //Procedemos a crear la relación con el Cliente
    let response = await crearRelacion({
      moduloDer: "Contacts",
      moduloIzq: "qtk_log_notificacion_cliente",
      idDer: idCliente,
      idIzq: notificacion.data.id
    });

    //Procedemos a crear la relación con el Negocio
    response = await crearRelacion({
      moduloDer: "qtk_negocio",
      moduloIzq: "qtk_log_notificacion_cliente",
      idDer: idNegocio,
      idIzq: notificacion.data.id
    });

    return response;
  } catch (ex) {
    console.log("Error en registrar Log Notificación Cliente: " + ex.message);
    throw ex;
  }
};

module.exports = { registrarLogNotificacionCliente };

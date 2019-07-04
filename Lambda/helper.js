const { legacyServiceCall, serviceCall, legacyLogin } = require("./serviceCall");

const formatSuiteCRMDateTime = dateTime => {
  return new Date(dateTime).toISOString().slice(0, 10) + " " + new Date(dateTime).toISOString().slice(11, 19);
};

const formatSuiteCRMDate = dateTime => {
  return new Date(dateTime).toISOString().slice(0, 10);
};

const crearModulo = async ({ modulo, postData }) => {
  try {
    const response = await serviceCall({
      path: "/Api/V8/module",
      postData
    });
    return response;
  } catch (ex) {
    console.log(`Error al crear registro de ${modulo}:   ${ex.message}`);
    throw ex;
  }
};

const actualizarModulo = async ({ modulo, postData }) => {
  try {
    const response = await serviceCall({
      path: "/Api/V8/module",
      postData,
      method: "PATCH"
    });
    return response;
  } catch (ex) {
    console.log(`Error al actualizar registro de ${modulo}:   ${ex.message}`);
    throw ex;
  }
};

const crearRelacion = async ({ moduloDer, moduloIzq, idDer, idIzq }) => {
  try {
    const response = await serviceCall({
      path: `/Api/V8/module/${moduloDer}/${idDer}/relationships`,
      postData: JSON.stringify({
        data: {
          type: moduloIzq,
          id: idIzq
        }
      }),
      method: "POST"
    });
    return response;
  } catch (ex) {
    console.log(`Error al crear registro de ${modulo}:   ${ex.message}`);
    throw ex;
  }
};

const legacyCrearRelacion = async ({ modulo, moduloId, nombreRelacion, relatedId }) => {
  try {
    const relationData = JSON.stringify({
      session: await legacyLogin(),
      module_name: modulo,
      module_id: moduloId,
      link_field_name: nombreRelacion,
      related_ids: [relatedId],
      name_value_list: [],
      delete: 0 //0: create   1: delete
    });
    const record = await legacyServiceCall({
      method: "set_relationship",
      argumentList: relationData
    });

    return record;
  } catch (ex) {
    console.log(`Error al crear relación de ${modulo} - ${nombreRelacion}:   ${ex.message}`);
    throw ex;
  }
};

const obtenerModulo = async ({
  modulo,
  id = null,
  campoId = null,
  nomUnico = null,
  campoNomUnico = null,
  campos = ["id"],
  relaciones = []
}) => {
  try {
    const moduleData = JSON.stringify({
      session: await legacyLogin(),
      module_name: modulo,
      query: id != null ? `${campoId}=${id}` : `${campoNomUnico}='${nomUnico}'`,
      order_by: "",
      offset: 0,
      select_fields: campos,
      link_name_to_fields_array: relaciones,
      max_results: 1,
      deleted: 0,
      Favorites: false
    });
    const record = await legacyServiceCall({
      method: "get_entry_list",
      argumentList: moduleData
    });

    return record.entry_list[0];
  } catch (ex) {
    console.log("Error en obtener Id de módulo: " + ex.message);
    throw ex;
  }
};

const obtenerRelacionesModulo = async ({ modulo, id, nombreRelacion }) => {
  try {
    const response = await serviceCall({
      path: `/Api/V8/module/${modulo}/${id}/relationships/${nombreRelacion}`,
      postData: JSON.stringify({ data: "" }),
      method: "GET"
    });
    return response;
  } catch (ex) {
    console.log("Error en obtener relaciones de módulo: " + ex.message);
    throw ex;
  }
};

const obtenerIdCliente = async idCliente => {
  try {
    return (await obtenerModulo({
      modulo: "Contacts",
      id: idCliente,
      campoId: "id_cliente_c"
    })).id;
  } catch (ex) {
    console.log(`No se encontró Cliente con id_cliente_c ${idCliente}`);
    throw ex;
  }
};

const obtenerIdLocal = async idLocal => {
  try {
    return (await obtenerModulo({
      modulo: "qtk_local",
      id: idLocal,
      campoId: "id_local_c"
    })).id;
  } catch (ex) {
    console.log(`No se encontró Local con id_local_c ${idLocal}`);
    throw ex;
  }
};

const obtenerIdNegocio = async idNegocio => {
  try {
    return (await obtenerModulo({
      modulo: "qtk_negocio",
      id: idNegocio,
      campoId: "id_negocio_c"
    })).id;
  } catch (ex) {
    console.log(`No se encontró Negocio con id_negocio_c ${idNegocio}`);
    throw ex;
  }
};

const obtenerIdNegocioIdLocal = async idLocal => {
  try {
    const negocio = await obtenerRelacionesModulo({
      modulo: "qtk_local",
      id: idLocal,
      nombreRelacion: "qtk_negocio_qtk_local_1"
    });
    return negocio.data[0].id;
  } catch (ex) {
    console.log(`No se encontró Negocio con qtk_negocio_qtk_local_1 ${idLocal}`);
    throw ex;
  }
};

const obtenerIdTipoEvento = async idTipoEvento => {
  try {
    return (await obtenerModulo({
      modulo: "qtk_tipo_evento",
      id: idTipoEvento,
      campoId: "id_tipo_evento_c"
    })).id;
  } catch (ex) {
    console.log(`No se encontró Tipo de Evento con id_tipo_evento_c ${idTipoEvento}`);
    throw ex;
  }
};

const obtenerIdCodigoCliente = async codigo => {
  try {
    return (await obtenerModulo({
      modulo: "qtk_codigo_cliente",
      nomUnico: codigo,
      campoNomUnico: "codigo_c"
    })).id;
  } catch (ex) {
    console.log(`No se encontró Código Cliente con código ${codigo}`);
    throw ex;
  }
};

module.exports = {
  formatSuiteCRMDateTime,
  formatSuiteCRMDate,
  obtenerModulo,
  crearModulo,
  actualizarModulo,
  crearRelacion,
  obtenerRelacionesModulo,
  obtenerIdCliente,
  obtenerIdLocal,
  obtenerIdNegocio,
  obtenerIdNegocioIdLocal,
  obtenerIdTipoEvento,
  legacyCrearRelacion,
  obtenerIdCodigoCliente
};

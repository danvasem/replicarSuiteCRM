const { crearCliente } = require("./cliente.js");
const { crearCodigoCliente, actualizarCodigoCliente } = require("./vincard");
const {
  registrarAcumulacion,
  registrarAfiliacion,
  registrarCuponJuego,
  registrarRedencion,
  registrarReverso
} = require("./eventos");
const AWS = require("aws-sdk");
const lambda = new AWS.Lambda();

exports.lambdaHandler = async (event, context) => {
  try {
    await importarClientes();
    return true;
  } catch (ex) {
    console.log("Error en la ejecución de la migración: " + ex.message);
    return false;
  }
};

const importarClientes = async () => {
  try {
    console.log("Iniciando importación de Clientes");
    //Obtenemos todos los Ids en VINCO
    const res = await consultarRDS("VINCO", "select IdCliente from VC_Cliente where IdCliente=605 limit 10");
    const data = JSON.parse(res.Payload);
    if (data.length > 0) {
      for (let i = 0; i < data.length; i++) {
        let idCliente = data[i].IdCliente;

        await importarCliente(idCliente);
        //await importarCodigoCliente(idCliente);
        //await importarAfiliaciones(idCliente);
        //await importarAcumulaciones(idCliente);
        await importarRedenciones(idCliente);
      }
    }
  } catch (ex) {
    console.log("Error en la importación de clientes: " + ex.message);
  }
};

const consultarRDS = async (database, query) => {
  const params = {
    FunctionName: "VP-ConsultarRDSFunction-NOP0PKLIF0FB",
    Payload: JSON.stringify({
      database: database,
      query: query
    })
  };

  const res = await lambda.invoke(params).promise();

  return res;
};

const importarCliente = async idCliente => {
  try {
    let cliente = await consultarRDS("VINCO", `select * from VC_Cliente where IdCliente=${idCliente};`);
    cliente = JSON.parse(cliente.Payload)[0];
    await crearCliente({
      NomUnicoCliente: cliente.sNombreUnico,
      IdRdsRegistro: cliente.IdCliente,
      Nombre: cliente.sNombre,
      Apellido: cliente.sApellido,
      CodigoSexo: cliente.sSexo,
      FechaNacimiento: cliente.dFechaNacimiento,
      CodigoCiudad: cliente.sCiudad,
      CodigoPais: cliente.sPais,
      Direccion: cliente.sDireccion,
      TelefonoMovil: cliente.sTelefonoMovil,
      CorreoElectronico: cliente.sCorreoElectronico,
      FechaCreacion: cliente.dFechaCreacion,
      FechaUltimaModificacion: cliente.dFechaUltimaActualizacion,
      FechaRegistro: cliente.dFechaRegistroCliente,
      AppRegistro: cliente.sAppRegistro,
      TipoLogin: cliente.sTipoLogin,
      Estado: cliente.sEstado
    });
    console.log(`Cliente ${idCliente} importado.`);
  } catch (ex) {
    console.log(`Error en la importación de Cliente ${idCliente}: ${ex.message}`);
  }
};

const importarCodigoCliente = async idCliente => {
  try {
    let cards = await consultarRDS("VINCO", `select * from VC_CodigoCliente where IdCliente=${idCliente};`);
    cards = JSON.parse(cards.Payload);
    if (cards.length > 0) {
      for (let i = 0; i < cards.length; i++) {
        let vincard = cards[i];
        await crearCodigoCliente({
          Codigo: vincard.sCodigo,
          FechaCreacion: vincard.dFechaCreacion,
          Estado: vincard.sEstado,
          IdLocal: { IdClaveForanea: vincard.IdLocal },
          IdNegocio: { IdClaveForanea: vincard.IdNegocio }
        });

        await actualizarCodigoCliente({
          Codigo: vincard.sCodigo,
          FechaActivacion: vincard.dFechaActivacion,
          Estado: vincard.sEstado,
          IdCliente: { IdClaveForanea: vincard.IdCliente },
          IdLocal: { IdClaveForanea: vincard.IdLocalAct },
          IdNegocio: { IdClaveForanea: vincard.IdNegocioAct }
        });

        console.log(`Vincard ${vincard.sCodigo} importado.`);
      }
    }
  } catch (ex) {
    console.log(`Error en la importación de Vincards: ${ex.message}`);
    throw ex;
  }
};

const importarAcumulaciones = async idCliente => {
  try {
    let acumulaciones = await consultarRDS(
      "VINCO",
      `select 
    E.dFechaCreacion,
    E.IdCliente,
    E.IdClienteResponsable,
    E.IdEvento,
    E.IdLocal,
    E.IdTipoEvento,
    E.IdUsuarioResponsable,
    E.mValor,
    E.sCodigoCliente,
    E.sEstado,
    E.sNumeroEventoRelacionado,
    E.sNumeroUnico,
    E.sTipoCodigoCliente,
    max(J.IdCampania) as IdCampania,
    sum(M.mValorCuenta)  as SaldoCuenta,
    sum(M.mValor) as AvancePartida
    from 
    VINCO.VC_Evento E
    inner join VINCO.VC_MovPartida M on M.IdEvento=E.IdEvento
    inner join VINCO.VC_Partida P on P.IdPartida=M.IdPartida
    inner join VINCO.VC_Juego J on J.IdJuego=P.IdJuego
    where E.IdTipoevento=1
    and E.IdCliente=${idCliente}
    group by 
    E.dFechaCreacion,
    E.IdCliente,
    E.IdClienteResponsable,
    E.IdEvento,
    E.IdLocal,
    E.IdTipoEvento,
    E.IdUsuarioResponsable,
    E.mValor,
    E.sCodigoCliente,
    E.sEstado,
    E.sNumeroEventoRelacionado,
    E.sNumeroUnico,
    E.sTipoCodigoCliente`
    );
    acumulaciones = JSON.parse(acumulaciones.Payload);
    if (acumulaciones.length > 0) {
      for (let i = 0; i < acumulaciones.length; i++) {
        let acumulacion = acumulaciones[i];
        await registrarAcumulacion(
          {
            NumeroUnico: acumulacion.sNumeroUnico,
            IdTipoEvento: { IdClaveForanea: acumulacion.IdTipoEvento },
            FechaCreacion: acumulacion.dFechaCreacion,
            Valor: acumulacion.mValor,
            IdUsuarioResponsable: { IdClaveForanea: acumulacion.IdUsuarioResponsable },
            Estado: acumulacion.sEstado,
            TipoCodigoCliente: acumulacion.sTipoCodigoCliente,
            CodigoCliente: acumulacion.sCodigoCliente,
            ValoresAcumulados: [
              {
                SaldoCuenta: acumulacion.SaldoCuenta,
                AvancePartida: acumulacion.AvancePartida
              }
            ],
            IdCliente: { IdClaveForanea: acumulacion.IdCliente },
            IdLocal: { IdClaveForanea: acumulacion.IdLocal }
          },
          acumulacion.IdCampania
        );
        console.log(`Acumulacion ${acumulacion.sNumeroUnico} importado.`);
      }
    }
  } catch (ex) {
    console.log(`Error en la importación de Acumulaciones: ${ex.message}`);
  }
};

const importarAfiliaciones = async idCliente => {
  try {
    let acumulaciones = await consultarRDS(
      "VINCO",
      `select 
    E.dFechaCreacion,
    E.IdCliente,
    E.IdClienteResponsable,
    E.IdEvento,
    E.IdLocal,
    E.IdTipoEvento,
    E.IdUsuarioResponsable,
    E.mValor,
    E.sCodigoCliente,
    E.sEstado,
    E.sNumeroEventoRelacionado,
    E.sNumeroUnico,
    E.sTipoCodigoCliente,
    max(J.IdCampania) as IdCampania,
    sum(M.mValorCuenta)  as SaldoCuenta,
    sum(M.mValor) as AvancePartida
    from 
    VINCO.VC_Evento E
    inner join VINCO.VC_MovPartida M on M.IdEvento=E.IdEvento
    inner join VINCO.VC_Partida P on P.IdPartida=M.IdPartida
    inner join VINCO.VC_Juego J on J.IdJuego=P.IdJuego
    where E.IdTipoevento=3
    and E.IdCliente=${idCliente}
    group by 
    E.dFechaCreacion,
    E.IdCliente,
    E.IdClienteResponsable,
    E.IdEvento,
    E.IdLocal,
    E.IdTipoEvento,
    E.IdUsuarioResponsable,
    E.mValor,
    E.sCodigoCliente,
    E.sEstado,
    E.sNumeroEventoRelacionado,
    E.sNumeroUnico,
    E.sTipoCodigoCliente`
    );
    afiliaciones = JSON.parse(acumulaciones.Payload);
    if (afiliaciones.length > 0) {
      for (let i = 0; i < afiliaciones.length; i++) {
        let afiliacion = afiliaciones[i];
        await registrarAfiliacion(
          {
            NumeroUnico: afiliacion.sNumeroUnico,
            IdTipoEvento: { IdClaveForanea: afiliacion.IdTipoEvento },
            FechaCreacion: afiliacion.dFechaCreacion,
            Valor: afiliacion.mValor,
            IdUsuarioResponsable: { IdClaveForanea: afiliacion.IdUsuarioResponsable },
            Estado: afiliacion.sEstado,
            TipoCodigoCliente: afiliacion.sTipoCodigoCliente,
            CodigoCliente: afiliacion.sCodigoCliente,
            ValoresAcumulados: [
              {
                SaldoCuenta: afiliacion.SaldoCuenta,
                AvancePartida: afiliacion.AvancePartida
              }
            ],
            IdCliente: { IdClaveForanea: afiliacion.IdCliente },
            IdLocal: { IdClaveForanea: afiliacion.IdLocal }
          },
          afiliacion.IdCampania
        );
        console.log(`Afiliacion ${afiliacion.sNumeroUnico} importado.`);
      }
    }
  } catch (ex) {
    console.log(`Error en la importación de Afiliaciones: ${ex.message}`);
  }
};

const importarRedenciones = async idCliente => {
  try {
    let redenciones = await consultarRDS(
      "VINCO",
      `select
      R.IdCliente,
      R.IdNegocio,
      R.IdLocal,
      R.IdCuenta,
      R.IdPremio,
      R.dFechaRedencion,
      R.mValor,
      R.mMontoReferencial,
      E.IdTipoEvento,
      E.IdUsuarioResponsable,
      E.sTipoCodigoCliente,
      E.sCodigoCliente,
      E.sNumeroUnico,
      E.sEstado,
      C.sNumeroUnico cuentaNumeroUnico
      from VINCO.VC_Redencion R
      inner join VINCO.VC_Evento E on E.IdEvento=R.IdEvento
      inner join VINCO.VC_Cuenta C on C.IdCuenta=R.IdCuenta
      where R.IdCliente=${idCliente};
      `
    );
    redenciones = JSON.parse(redenciones.Payload);
    if (redenciones.length > 0) {
      for (let i = 0; i < redenciones.length; i++) {
        let redencion = redenciones[i];
        await registrarRedencion(
          {
            FechaRedencion: redencion.dFechaRedencion,
            Valor: redencion.mValor,
            MontoReferncial: redencion.mMontoReferencial,
            IdCliente: { IdClaveForanea: redencion.IdCliente },
            IdNegocio: { IdClaveForanea: redencion.IdNegocio },
            IdLocal: { IdClaveForanea: redencion.IdLocal },
            IdPremio: { IdClaveForanea: redencion.IdPremio }
          },
          {
            IdTipoEvento: { IdClaveForanea: redencion.IdTipoEvento },
            IdUsuarioResponsable: { IdClaveForanea: redencion.IdUsuarioResponsable },
            TipoCodigoCliente: redencion.sTipoCodigoCliente,
            CodigoCliente: redencion.sCodigoCliente,
            NumeroUnico: redencion.sNumeroUnico,
            Estado: redencion.sEstado
          },
          {
            NumeroUnico: redencion.cuentaNumeroUnico
          }
        );
        console.log(`Redencion ${redencion.sNumeroUnico} importado.`);
      }
    }
  } catch (ex) {
    console.log(`Error en la importación de Redenciones: ${ex.message}`);
  }
};

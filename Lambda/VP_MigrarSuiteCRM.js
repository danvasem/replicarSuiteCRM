const { crearCliente, crearCalificacionNegocio, crearClienteNegocio } = require("./cliente.js");
const { crearCodigoCliente, actualizarCodigoCliente } = require("./vincard");
const { crearCuenta } = require("./cuenta");
const { registrarLogNotificacionCliente } = require("./logNotificacionCliente");
const { crearPartida } = require("./partida");
const {
  registrarAcumulacion,
  registrarAfiliacion,
  registrarCuponJuego,
  registrarRedencion,
  registrarReverso
} = require("./eventos");
const { resetearCredenciales } = require("./serviceCall");
const AWS = require("aws-sdk");
const lambda = new AWS.Lambda();
const DATABASE_NAME = process.env.VINCO_DATABASE_NAME;

exports.lambdaHandler = async (event, context) => {
  try {
    await ejecutarImportacion(event.IdCliente);
    return true;
  } catch (ex) {
    console.log("Error en la ejecución de la migración: " + ex.message);
    return "Error: " + ex.message + " - " + ex.stack;
  }
};

const ejecutarImportacion = async idCliente => {
  try {
    console.log("Iniciando importación de Clientes");
    resetearCredenciales();
    //Obtenemos todos los Ids en VINCO
    const res = await consultarRDS(
      DATABASE_NAME,
      `select IdCliente from VC_Cliente where IdCliente=${idCliente} limit 10`
    );
    const data = JSON.parse(res.Payload);
    if (data.length > 0) {
      for (let i = 0; i < data.length; i++) {
        let idCliente = data[i].IdCliente;

        await importarCliente(idCliente);
        await importarCodigoCliente(idCliente);
        await importarCuentas(idCliente);
        await importarCalificacionNegocio(idCliente);
        await importarClienteNegocio(idCliente);
        await importarAfiliaciones(idCliente);
        await importarAcumulaciones(idCliente);
        await importarRedenciones(idCliente);
        await importarReversos(idCliente);
        await importarCuponesJuego(idCliente);
        await importarLogNotificacionCliente(idCliente);
        await importarPartidas(idCliente);
      }
    }
  } catch (ex) {
    console.log("Error en ejecutar importación: " + ex.message);
    throw ex;
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
    await importarRegistros(`select * from VC_Cliente where IdCliente=${idCliente};`, async cliente => {
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
    });
  } catch (ex) {
    console.log(`Error en la importación de Cliente ${idCliente}: ${ex.message}`);
    throw ex;
  }
};

const importarCodigoCliente = async idCliente => {
  try {
    await importarRegistros(`select * from VC_CodigoCliente where IdCliente=${idCliente};`, async vincard => {
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
    });
  } catch (ex) {
    console.log(`Error en la importación de Vincards: ${ex.message}`);
    throw ex;
  }
};

const importarAcumulaciones = async idCliente => {
  try {
    await importarRegistros(
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
    VC_Evento E
    inner join VC_MovPartida M on M.IdEvento=E.IdEvento
    inner join VC_Partida P on P.IdPartida=M.IdPartida
    inner join VC_Juego J on J.IdJuego=P.IdJuego
    where E.IdTipoevento=1
    and E.sEstado='A'
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
    E.sTipoCodigoCliente`,
      async acumulacion => {
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
    );
  } catch (ex) {
    console.log(`Error en la importación de Acumulaciones: ${ex.message}`);
    throw ex;
  }
};

const importarAfiliaciones = async idCliente => {
  try {
    await importarRegistros(
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
      from VC_Evento E
      inner join VC_MovPartida M on M.IdEvento=E.IdEvento
      inner join VC_Partida P on P.IdPartida=M.IdPartida
      inner join VC_Juego J on J.IdJuego=P.IdJuego
      where E.IdTipoevento=3
      and E.sEstado='A'
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
      E.sTipoCodigoCliente`,
      async afiliacion => {
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
    );
  } catch (ex) {
    console.log(`Error en la importación de Afiliaciones: ${ex.message}`);
    throw ex;
  }
};

const importarRedenciones = async idCliente => {
  try {
    await importarRegistros(
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
      from VC_Redencion R
      inner join VC_Evento E on E.IdEvento=R.IdEvento
      inner join VC_Cuenta C on C.IdCuenta=R.IdCuenta
      where E.sEstado='A'
      and R.IdCliente=${idCliente};
      `,
      async redencion => {
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
    );
  } catch (ex) {
    console.log(`Error en la importación de Redenciones: ${ex.message}`);
    throw ex;
  }
};

const importarReversos = async idCliente => {
  try {
    await importarRegistros(
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
      R.IdTipoEvento IdTipoEventoRelacionado,
      R.mValor mValorReversado,
      R.dFechaCreacion dFechaEventoReversado,
      RD.IdPremio IdPremioReversado
      from VC_Evento E
      inner join VC_Evento R on R.sNumeroUnico=E.sNumeroEventoRelacionado
      left outer join VC_Redencion RD on RD.IdEvento=R.IdEvento
      where E.IdTipoevento=4 
      and E.sNumeroEventoRelacionado is not null
      and E.IdCliente=${idCliente}`,
      async reverso => {
        await registrarReverso(
          {
            IdCliente: { IdClaveForanea: reverso.IdCliente },
            IdLocal: { IdClaveForanea: reverso.IdLocal },
            IdTipoEvento: { IdClaveForanea: reverso.IdTipoEvento },
            IdUsuarioResponsable: { IdClaveForanea: reverso.IdUsuarioResponsable },
            NumeroUnico: reverso.sNumeroUnico,
            FechaCreacion: reverso.dFechaCreacion,
            Estado: reverso.sEstado,
            ValorReversado: reverso.mValorReversado,
            FechaEventoReversado: reverso.dFechaEventoReversado,
            IdPremioReversado: reverso.IdPremioReversado
          },
          {
            IdTipoEvento: { IdClaveForanea: reverso.IdTipoEventoRelacionado },
            NumeroUnico: reverso.sNumeroEventoRelacionado
          }
        );
        console.log(`Reverso ${reverso.sNumeroUnico} importado.`);
      }
    );
  } catch (ex) {
    console.log(`Error en la importación de Reversos: ${ex.message}`);
    throw ex;
  }
};

const importarCuponesJuego = async idCliente => {
  try {
    await importarRegistros(
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
      sum(M.mValor) as AvancePartida,
      J.sCodigoCuponJuego sCodigoCuponJuego
      from 
      VC_Evento E
      inner join VC_MovPartida M on M.IdEvento=E.IdEvento
      inner join VC_Partida P on P.IdPartida=M.IdPartida
      inner join VC_Juego J on J.IdJuego=P.IdJuego
      where E.IdTipoevento=5
      and E.sEstado='A'
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
      E.sTipoCodigoCliente`,
      async cupon => {
        await registrarCuponJuego(
          {
            NumeroUnico: cupon.sNumeroUnico,
            IdTipoEvento: { IdClaveForanea: cupon.IdTipoEvento },
            FechaCreacion: cupon.dFechaCreacion,
            Valor: cupon.mValor,
            IdUsuarioResponsable: { IdClaveForanea: cupon.IdUsuarioResponsable },
            Estado: cupon.sEstado,
            TipoCodigoCliente: cupon.sTipoCodigoCliente,
            CodigoCliente: cupon.sCodigoCliente,
            ValoresAcumulados: [
              {
                SaldoCuenta: cupon.SaldoCuenta,
                AvancePartida: cupon.AvancePartida
              }
            ],
            IdCliente: { IdClaveForanea: cupon.IdCliente },
            IdLocal: { IdClaveForanea: cupon.IdLocal },
            CodigoCupon: cupon.sCodigoCuponJuego
          },
          cupon.IdCampania
        );
        console.log(`Cupón ${cupon.sNumeroUnico} importado.`);
      }
    );
  } catch (ex) {
    console.log(`Error en la importación de Cupones Juego: ${ex.message}`);
    throw ex;
  }
};

const importarCuentas = async idCliente => {
  try {
    await importarRegistros(`select * from VC_Cuenta where IdCliente=${idCliente}`, async cuenta => {
      await crearCuenta({
        NumeroUnico: cuenta.sNumeroUnico,
        SaldoDisponible: cuenta.mSaldoDisponible,
        SaldoDisponibleBase: cuenta.mSaldoDisponibleBase,
        SaldoContable: cuenta.mSaldoContable,
        SaldoContableBase: cuenta.mSaldoContableBase,
        FechaApertura: cuenta.dFechaApertura,
        FechaVigencia: cuenta.dFechaVigencia,
        FechaExpiracion: cuenta.dFechaExpiracion,
        Estado: cuenta.sEstado,
        IdCliente: { IdClaveForanea: cuenta.IdCliente },
        IdNegocio: { IdClaveForanea: cuenta.IdNegocio }
      });
      console.log(`Cuenta ${cuenta.sNumeroUnico} importada`);
    });
  } catch (ex) {
    console.log(`Error en la importación de cuentas: ${ex.message}`);
    throw ex;
  }
};

const importarCalificacionNegocio = async idCliente => {
  try {
    await importarRegistros(
      `select * from VC_ClienteNegocioCalificacion where IdCliente=${idCliente}`,
      async calificacion => {
        await crearCalificacionNegocio({
          Rating: calificacion.iCalificacion,
          FechaCreacion: calificacion.dFecha,
          IdCliente: { IdClaveForanea: calificacion.IdCliente },
          IdNegocio: { IdClaveForanea: calificacion.IdNegocio }
        });
        console.log(`Calificación a negocio ${calificacion.IdNegocio} importada`);
      }
    );
  } catch (ex) {
    console.log(`Error en la importación de Calificaciones Negocio: ${ex.message}`);
    throw ex;
  }
};

const importarClienteNegocio = async idCliente => {
  try {
    await importarRegistros(`select * from VC_ClienteNegocio where IdCliente=${idCliente};`, async negocio => {
      await crearClienteNegocio({
        FechaCreacion: negocio.dFechaCreacion,
        IdCliente: { IdClaveForanea: negocio.IdCliente },
        IdNegocio: { IdClaveForanea: negocio.IdNegocio }
      });
      console.log(`Cliente ${negocio.IdCliente} Negocio ${negocio.IdNegocio} importado.`);
    });
  } catch (ex) {
    console.log(`Error en la importación de Cliente Negocio: ${ex.message}`);
    throw ex;
  }
};

const importarLogNotificacionCliente = async idCliente => {
  try {
    await importarRegistros(
      `select * from VC_LogNotificacionCliente where IdCliente=${idCliente};`,
      async notificacion => {
        await registrarLogNotificacionCliente({
          Titulo: notificacion.sTitulo,
          FechaEnvio: notificacion.dFechaEnvio,
          Mensaje: notificacion.sMensaje,
          NombreUnicoGrupo: notificacion.sNombreUnicoGrupo,
          Error: notificacion.sError,
          Canal: notificacion.sCanal,
          Estado: notificacion.sEstado,
          IdCliente: { IdClaveForanea: notificacion.IdCliente },
          IdNegocio: { IdClaveForanea: notificacion.IdNegocio }
        });
        console.log(`Log notificación cliente ${notificacion.sTitulo} importado.`);
      }
    );
  } catch (ex) {
    console.log(`Error en la importación de Log Notificación Cliente: ${ex.message}`);
    throw ex;
  }
};

const importarPartidas = async idCliente => {
  try {
    await importarRegistros(
      `select
      distinct
      P.dFechaCreacion,
      P.dFechaFin,
      P.fProgreso,
      P.IdCliente,
      P.IdClienteAdmin,
      P.IdJuego,
      P.IdPartida,
      P.iRepeticion,
      P.mValorAlcanzado,
      P.mValorCliente,
      P.sEstado,
      P.sNumeroUnico,
      J.IdCampania,
      C.IdNegocio
      from VC_Partida P 
      inner join VC_Juego J on J.IdJuego=P.IdJuego
      inner join VC_Campania C on C.IdCampania=J.IdCampania
      inner join VC_MovPartida M on M.IdPartida=P.IdPartida
      inner join VC_Evento E on E.IdEvento=M.IdEvento 
          and E.sEstado='A' 
      and P.IdCliente=${idCliente}`,
      async partida => {
        await crearPartida(
          {
            NumeroUnico: partida.sNumeroUnico,
            ValorAlcanzado: partida.mValorAlcanzado,
            Progreso: partida.fProgreso,
            ValorCliente: partida.mValorCliente,
            FechaCreacion: partida.dFechaCreacion,
            FechaFin: partida.dFechaFin,
            Estado: partida.sEstado,
            Repeticion: partida.iRepeticion,
            IdCliente: { IdClaveForanea: partida.IdCliente }
          },
          {
            IdNegocio: { IdClaveForanea: partida.IdNegocio },
            IdCampania: { IdClaveForanea: partida.IdCampania }
          }
        );
        console.log(`Partida ${partida.sNumeroUnico} importado.`);
      }
    );
  } catch (ex) {
    console.log(`Error en la importación de Partida: ${ex.message}`);
  }
};

const importarRegistros = async (query, importacion) => {
  let resultados = await consultarRDS(DATABASE_NAME, query);
  resultados = JSON.parse(resultados.Payload);
  if (resultados.length > 0) {
    for (let i = 0; i < resultados.length; i++) {
      let registro = resultados[i];
      await importacion(registro);
    }
  }
};

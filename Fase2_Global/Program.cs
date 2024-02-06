using Fase2_Global.Importadores;
using Fase2_Global.Utilidades;
using Fase2_Global.Models.EnumsModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System;
using System.Drawing.Text;
using System.Runtime.CompilerServices;

namespace Fase2_Global
{
    internal class Program
    {
        private static IConfiguration configuration;

        private static Microsoft.Extensions.Logging.ILogger<Program> Logger;

        private static ServiceCollection serviceCollection;

        private static ServiceProvider serviceProvider;

        static void Main(string[] args)
        {
            Console.WriteLine("Cargador de origenes ");
            if (args.Length == 0)
            {
                Console.WriteLine(
                $@"Parámetros disponibles:
                
                --B2C-45: Carga fichero suelto. Necesario modificar código para lectura de columnas personalizadas en tablas concretas.
                --B2C-44: Carga los ficheros diarios de Facturación Agrupada (Origen 1)                
                --B2C-43: Carga los ficheros diarios de Seguimiento de Facturas (Origen 4)
                --B2C-10: Carga los ficheros diarios de Rechazos (Origen 5)
                --B2C-8: Carga los archivos diarios Drafs en estado Borrador (Origen 8)
                ");
            }

            Startup();


            var database = serviceProvider.GetService<Database>();
            var generarEmail = serviceProvider.GetService<GenerarMail>();
            var folderImporter = serviceProvider.GetService<FolderImporter>();
            var email = serviceProvider.GetService<Email>();
            var familyService = serviceProvider.GetService<FamilyManager>();
            string folder;

            try
            {
                var pro = serviceProvider.GetService<ProcedureManager>();

                foreach (var arg in args)
                {

                    database.BeginTransaction();

                    switch (arg.ToUpper()) 
                    {
                        case "--B2C-FASE2":
                            folder = configuration.GetValue<string>("InputFolderBase").Replace("@USUARIO", Environment.UserName) + "\\08. Drafts en estado Borrador";
                            folderImporter.ExecuteDraftsDiarios(database, folder);
                            folder = configuration.GetValue<string>("InputFolderBase").Replace("@USUARIO", Environment.UserName) + "\\10. Suscripciones bloqueadas";
                            folderImporter.ExecuteSuscBloqueadas(database, folder);
                            folder = configuration.GetValue<string>("InputFolderBase").Replace("@USUARIO", Environment.UserName) + "\\43. Owner Transfer\\Cargar";
                            folderImporter.ExecuteOwnerTransfer(database, folder);
                            folder = configuration.GetValue<string>("InputFolderBase").Replace("@USUARIO", Environment.UserName) + "\\44. Mapeo de Productos\\Cargar";
                            folderImporter.ExecuteMapeoProductos(database, folder);
                            folder = configuration.GetValue<string>("InputFolderBase").Replace("@USUARIO", Environment.UserName) + "\\45. Fecha ultima Factura\\Cargar";
                            folderImporter.ExecuteFechaUltFactura(database, folder);

                            var procedure = serviceProvider.GetService<ProcedureManager>();
                            procedure.ExecuteProcedures(database, "Script_Fases1_2.sql");

                            database.CommitTransaction();

                            database.BeginTransaction();

                            var report = serviceProvider.GetService<Reportes>();
                            report.Execute();

                            database.CommitTransaction();

                            database.BeginTransaction();

                            var enviarEmail = serviceProvider.GetService<EnviarEmail>();
                            enviarEmail.Execute();

                            break;


                        default:
                            Console.WriteLine($"Parámetro desconocido {arg}");
                            break;
                    }

                    database.CommitTransaction();
                }
#if DEBUG
                //pro = serviceProvider.GetService<ProcedureManager>();
                //pro.ExecuteProcedures(database, "vaciar tablas temporales.sql");
#endif
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
                database.RollbackTransaction();
                throw e;
            }
            finally
            {
                database.Dispose();
            }

            try
            {
                #region Envio mail
                //Enviar mail con el informe                
                //Logger.LogInformation("--I-- Generar los reporte y envía email de finalización de la carga");
                //var reports = folderImporter.GetReportsToMail();
                //var fechaReporte = "";

                //if (reports != null & reports.Count > 0)
                //{

                //    if (reports.ContainsKey("--zuora"))
                //    {
                //        generarEmail.GenerateReportToMail(reports["--zuora"][0].ToString("yyyyMMdd"));
                //        fechaReporte = reports["--zuora"][0].ToString("yyyy-MM-dd");
                //        email.Execute($"Proceso de Cargas de No Energéticos Zuora {fechaReporte}", "Cargas");
                //    }
                //    if (reports.ContainsKey("--pendientes"))
                //    {
                //        fechaReporte = reports["--pendientes"][0].ToString("yyyy-MM-dd");
                //        email.Execute($"Proceso de Cruces de No Energéticos Zuora {fechaReporte}", "Cruces");
                //    }

                //}
                #endregion
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
            }

            //Cerramos pulsando tecla
            //Console.WriteLine("Proceso finalizado. Pulse cualquier tecla para cerrar."); 
            //Console.ReadKey();
        }

        private static void Startup()
        {
            // inicializa la inyección de dependencias

            serviceCollection = new ServiceCollection();

            // Agrega el log

            var loggerConfiguration = new LoggerConfiguration()
              .Enrich.FromLogContext()
              .WriteTo.Console()
              .WriteTo.File("NoEnergeticoBILauncher.log", rollingInterval: RollingInterval.Day);

            Log.Logger = loggerConfiguration.CreateLogger();

            serviceCollection.AddLogging(c =>
            {
                c.AddSerilog(dispose: true);
            });

            // Agrega la configuración

            configuration = new ConfigurationBuilder()
                .AddJsonFile("appSettings.json", false, true)
                .Build();

            serviceCollection.AddSingleton<IConfiguration>(configuration);
            serviceCollection.AddSingleton<Configurator>();
            serviceCollection.AddTransient<ImportadorRechazosDiarios>();
            serviceCollection.AddTransient<ImportadorRechazosSemanalB2B>();
            serviceCollection.AddTransient<ImportadorDraftsDiarios>();
            serviceCollection.AddTransient<ImportadorDraftsSemanalB2B>();
            serviceCollection.AddTransient<ImportadorFacturacionAgrupadaDiarias>();
            serviceCollection.AddTransient<ImportadorFacturacionAgrupadaSemanalB2B>();
            serviceCollection.AddTransient<ImportadorPURLSemanal>();
            serviceCollection.AddTransient<ImportadorFacturasEnCDCDiarias>();
            serviceCollection.AddTransient<ImportadorSeguimientoDiarias>();
            serviceCollection.AddTransient<ImportadorSeguimientoSemanalB2B>();
            serviceCollection.AddTransient<ImportadorSuscripcionesExpiradasMensual>();
            serviceCollection.AddTransient<ImportadorNotasCreditoDiarias>();
            serviceCollection.AddTransient<ImportadorNotasCreditoSemanalB2B>();
            serviceCollection.AddTransient<ImportadorCargaSuelta>();
            serviceCollection.AddTransient<ImportadorDraftMotivo>();
            serviceCollection.AddTransient<ImportadorOwnerTransfer>();
            serviceCollection.AddTransient<ImportadorPendienteFacturar>();
            serviceCollection.AddTransient<ImportadorSuscripcionesBloqueadas>();
            serviceCollection.AddTransient<ImportadorMapeoProductos>();
            serviceCollection.AddTransient<ImportadorFechaUltimaFactura>();
            serviceCollection.AddTransient<ImportadorUsuarios>();
            serviceCollection.AddTransient<FolderImporter>();
            serviceCollection.AddTransient<Email>();
            serviceCollection.AddTransient<EnviarEmail>();
            serviceCollection.AddTransient<Database>();
            serviceCollection.AddTransient<Exporter>();
            serviceCollection.AddTransient<ProcedureManager>();
            serviceCollection.AddTransient<FamilyManager>();
            serviceCollection.AddTransient<Reportes>();
            serviceCollection.AddTransient<GenerateReports>();
            //serviceCollection.AddTransient<CrucesExternos>();
            //serviceCollection.AddTransient<GenerarMail>();
            //serviceCollection.AddTransient<Conteos>();

            serviceProvider = serviceCollection.BuildServiceProvider();

            Logger = serviceProvider.GetService<ILogger<Program>>();
        }
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
//using Etl.ConsultasBI.Client;
using Microsoft.Extensions.Configuration;
using Fase2_Global.Utilidades;

namespace Fase2_Global.Importadores
{


    public class FolderFile
    {
        public string FullName { get; set; }
        public string Name { get; set; }
        public List<string> Partes { get; set; }
        public string Tipo { get; set; }
    }

    public class CsvFile
    {
        public string FullName { get; set; }
        public DateTime Date { get; set; }

        public List<CsvData> CsvDataList { get; set; }
    }

    public class CsvData
    {
        public string RecordType { get; set; }
        //public string InvoiceNumber { get; set; }
        public DateTime PostedDate { get; set; }
        public string Code { get; set; }
        public int Conteo { get; set; }
        public DateTime CommodityDate { get; set; }
    }


    public class FolderImporter
    {
        protected ILogger Logger;
        private IServiceProvider ServiceProvider;
        private Dictionary<string, List<DateTime>> _reportsToMail = new Dictionary<string, List<DateTime>>();
        //Dictionary<string, FacturaDuplicada> dicFacturasDuplicadas = new Dictionary<string, FacturaDuplicada>();
        public FolderImporter(ILogger<Importador> logger, IServiceProvider serviceProvider)
        {
            this.Logger = logger;
            this.ServiceProvider = serviceProvider;
        }

        public void ExecuteRechazosDiarios(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorRechazosDiarios importer;
            importer = ServiceProvider.GetService<ImportadorRechazosDiarios>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            List<string> lineasErroneas = importer.ExecuteRechazosDiarios(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente. Se encontraron {lineasErroneas.Count} líneas erróneas.");

                            if (lineasErroneas.Count > 0)
                            {
                                System.IO.File.WriteAllLines($"{dir}\\{file.Name}-ERROR.txt", lineasErroneas);
                            }
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteRechazosSemanalB2B(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorRechazosSemanalB2B importer;
            importer = ServiceProvider.GetService<ImportadorRechazosSemanalB2B>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            List<string> lineasErroneas = importer.ExecuteRechazosSemanalB2B(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente. Se encontraron {lineasErroneas.Count} líneas erróneas.");

                            if (lineasErroneas.Count > 0)
                            {
                                System.IO.File.WriteAllLines($"{dir}\\{file.Name}-ERROR.txt", lineasErroneas);
                            }
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecutePURLSemanal(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorPURLSemanal importer;
            importer = ServiceProvider.GetService<ImportadorPURLSemanal>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecutePURLSemanal(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteDraftsDiarios(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorDraftsDiarios importer;
            importer = ServiceProvider.GetService<ImportadorDraftsDiarios>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}' y '{importer.GetTableBBDD2()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                    database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteDraftsDiarios(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }                

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteDraftsSemanalB2B(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorDraftsSemanalB2B importer;
            importer = ServiceProvider.GetService<ImportadorDraftsSemanalB2B>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}' y '{importer.GetTableBBDD2()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                    database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteDraftsSemanalB2B(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }                

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteDraftMotivos(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorDraftMotivo importer;
            importer = ServiceProvider.GetService<ImportadorDraftMotivo>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteDraftMotivos(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //ARL:21/09/2023

        public void ExecutePendienteFacturar(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorPendienteFacturar importer;
            importer = ServiceProvider.GetService<ImportadorPendienteFacturar>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        var ultimo_archivo = (from f in dir.GetFiles() orderby f.LastWriteTime descending select f).First();

                        var prueba = ultimo_archivo.LastWriteTime;
                        string data = prueba.ToString();
                        var sFechaFichero = data.Substring(0,10); //    "27/09/2023 9:41:51"

                        if (DateTime.TryParseExact(sFechaFichero, "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    //var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        var prueba = file.LastWriteTime;
                        string data = prueba.ToString();
                        var sFechaFichero = data.Substring(0, 10);

                        //Obtenemos fecha del fichero
                        //var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecutePendienteFacturar(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesadoSinFecha(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ExecuteOwnerTransfer(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorOwnerTransfer importer;
            importer = ServiceProvider.GetService<ImportadorOwnerTransfer>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteOwnerTransfer(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ExecuteMapeoProductos(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorMapeoProductos importer;
            importer = ServiceProvider.GetService<ImportadorMapeoProductos>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteMapeoProductos(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ExecuteSuscBloqueadas(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorSuscripcionesBloqueadas importer;
            importer = ServiceProvider.GetService<ImportadorSuscripcionesBloqueadas>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteSuscBloqueadas(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ExecuteFechaUltFactura(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorFechaUltimaFactura importer;
            importer = ServiceProvider.GetService<ImportadorFechaUltimaFactura>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteFechaUltFactura(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ExecuteUsuariosOrigen(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorUsuarios importer;
            importer = ServiceProvider.GetService<ImportadorUsuarios>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"TRUNCATE TABLE {importer.GetTableBBDD()}");
                    //database.Execute($"truncate table {importer.GetTableBBDD2()}");

                    //Solo se debe procesar el fichero más actual.
                    //Comprobamos el fichero más actual y descartamos el resto.
                    IDictionary<FileInfo, DateTime> ficheros = new Dictionary<FileInfo, DateTime>();
                    foreach (var file in files)
                    {
                        //Obtenemos la fecha del fichero
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }
                        ficheros.Add(file, dtFechaFichero);
                    }
                    //Obtenermos la fecha más alta
                    var fechaReciente = ficheros.OrderByDescending(kvp => kvp.Value).First();
                    var listaFicheros = from x in ficheros where x.Value == fechaReciente.Value select x.Key;

                    foreach (var file in listaFicheros)
                    {
                        //Obtenemos nombre fichero sin extension.
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                        //Obtenemos fecha del fichero
                        var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                        if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                        {
                            dtFechaFichero = dt1;
                        }
                        else
                        {
                            if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                            {
                                dtFechaFichero = dt2;
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                            }
                        }

                        //Si no se ha procesado, lo procesamos.
                        if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                        {
                            //Comprobamos que el fichero no esté vacío.
                            if (file.Length > 0)
                            {
                                Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                                importer.ExecuteUsuariosOrigen(database, file.FullName, dtFechaFichero);
                                Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                            }
                            else
                            {
                                throw new Exception($"Carga Cancelada. El fichero '{file.Name}' está vacío.");
                            }
                        }
                        else
                        {
                            Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                        }
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        //FIN ANDRES

        //USADO PARA CARGAR FICHEROS SUELTOS CON CAMPOS PERSONALIZADOS
        public void ExecuteCargaSuelta(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorCargaSuelta importer;
            importer = ServiceProvider.GetService<ImportadorCargaSuelta>();

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    //Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    //database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Comprobamos que el fichero no esté vacío.
                    if (file.Length > 0)
                    {
                        Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                        importer.ExecuteCargaSuelta(database, file.FullName);
                        Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                    }
                    else
                    {
                        throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteFacturacionAgrupadaDiarias(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorFacturacionAgrupadaDiarias importer;
            importer = ServiceProvider.GetService<ImportadorFacturacionAgrupadaDiarias>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}' y '{importer.GetTableBBDD2()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                    database.Execute($"truncate table {importer.GetTableBBDD2()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteFacturacionAgrupadaDiarias(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteFacturacionAgrupadaSemanalB2B(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorFacturacionAgrupadaSemanalB2B importer;
            importer = ServiceProvider.GetService<ImportadorFacturacionAgrupadaSemanalB2B>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}' y '{importer.GetTableBBDD2()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                    database.Execute($"truncate table {importer.GetTableBBDD2()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteFacturacionAgrupadaSemanalB2B(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteFacturasEnCDCDiarias(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorFacturasEnCDCDiarias importer;
            importer = ServiceProvider.GetService<ImportadorFacturasEnCDCDiarias>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }



                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteFacturasEnCDCDiarias(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteNotasCreditoDiarias(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorNotasCreditoDiarias importer;
            importer = ServiceProvider.GetService<ImportadorNotasCreditoDiarias>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }



                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteNotasCreditoDiarias(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteNotasCreditoSemanalB2B(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorNotasCreditoSemanalB2B importer;
            importer = ServiceProvider.GetService<ImportadorNotasCreditoSemanalB2B>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }



                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteNotasCreditoSemanalB2B(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteSuscripcionesExpiradasMensual(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorSuscripcionesExpiradasMensual importer;
            importer = ServiceProvider.GetService<ImportadorSuscripcionesExpiradasMensual>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteSuscripcionesExpiradasMensual(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteSeguimientoDiarias(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorSeguimientoDiarias importer;
            importer = ServiceProvider.GetService<ImportadorSeguimientoDiarias>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteSeguimientoDiarias(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        public void ExecuteSeguimientoSemanalB2B(Database database, string folder)
        {
            var dir = new DirectoryInfo(folder);
            var files = dir.GetFiles("*.csv").ToList();
            var dias = new Dictionary<DateTime, List<FolderFile>>();
            ImportadorSeguimientoSemanalB2B importer;
            importer = ServiceProvider.GetService<ImportadorSeguimientoSemanalB2B>();
            DateTime dtFechaFichero;

            try
            {
                //Truncamos Tablas si existen archivos a procesar (evitar borrar sin cargar nada)
                if (files.Count > 0)
                {
                    Logger.LogInformation($"Limpiamos tablas de entrada: '{importer.GetTableBBDD()}'.");
                    database.Execute($"truncate table {importer.GetTableBBDD()}");
                }

                foreach (var file in files)
                {
                    //Obtenemos nombre fichero sin extension.
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    //Obtenemos fecha del fichero
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    //Si no se ha procesado, lo procesamos.
                    if ((int)database.GetData($"SELECT count(*) FROM FICHEROS_PROCESADOS WHERE NombreFichero='{file.Name.Trim()}'").Rows[0][0] == 0)
                    {
                        //Comprobamos que el fichero no esté vacío.
                        if (file.Length > 0)
                        {
                            Logger.LogInformation($"Procesando fichero '{file.Name}'. Comienza lectura de datos.");
                            importer.ExecuteSeguimientoSemanalB2B(database, file.FullName, dtFechaFichero);
                            Logger.LogInformation($"Carga Finalizada. Fichero '{file.Name}' procesado correctamente.");
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' está vacío.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation($"Ignorando fichero {file.Name} porque ya está en el sistema cargado.");
                    }
                }

                //Movemos los ficheros a la carpeta Mes/Año correspondiente una vez Procesado
                MoverFicheroProcesado(files, dir);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            //_reportsToMail.Add("--zuora", fechaprocesoList);
        }

        private void MoverFicheroProcesado(List<FileInfo> files, DirectoryInfo dir)
        {
            try
            {
                foreach (var file in files)
                {
                    //Obtenemos nombre y fecha del fichero
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();
                    var sFechaFichero = nameWithoutExtension.Substring(nameWithoutExtension.Count() - 8);
                    DateTime dtFechaFichero;

                    if (DateTime.TryParseExact(sFechaFichero, "ddMMyyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{ file.Name }' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    string carpetaDestino = CalcularCarpetaProcesado(dir, dtFechaFichero);

                    //Creamos carpeta, si existe no hace nada.
                    Directory.CreateDirectory(carpetaDestino);

                    file.MoveTo($"{carpetaDestino}\\{file.Name}", true);

                    Logger.LogInformation($"Fichero movido a la ruta: '{carpetaDestino}\\{file.Name}'");
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"ERROR. Error al mover fichero procesado: {ex.Message}");
            }
        }

        private void MoverFicheroProcesadoSinFecha(List<FileInfo> files, DirectoryInfo dir)
        {
            try
            {
                foreach (var file in files)
                {
                    //Obtenemos nombre y fecha del fichero
                    var nameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name).Trim();

                    var prueba = file.LastWriteTime;
                    string data = prueba.ToString();
                    var sFechaFichero = data.Substring(0, 10);
                    DateTime dtFechaFichero;

                    if (DateTime.TryParseExact(sFechaFichero, "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt1))
                    {
                        dtFechaFichero = dt1;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(sFechaFichero, "yyyy/MM/dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt2))
                        {
                            dtFechaFichero = dt2;
                        }
                        else
                        {
                            throw new Exception($"Carga Cancelada. El fichero '{file.Name}' no tiene la estructura del nombre correcta (....XXXXXXXX.csv).");
                        }
                    }

                    string carpetaDestino = CalcularCarpetaProcesado(dir, dtFechaFichero);

                    //Creamos carpeta, si existe no hace nada.
                    Directory.CreateDirectory(carpetaDestino);

                    file.MoveTo($"{carpetaDestino}\\{file.Name}", true);

                    Logger.LogInformation($"Fichero movido a la ruta: '{carpetaDestino}\\{file.Name}'");
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"ERROR. Error al mover fichero procesado: {ex.Message}");
            }
        }

        private string CalcularCarpetaProcesado(DirectoryInfo dir, DateTime dtFechaFichero)
        {
            string carpetaOrigen = Directory.GetParent(dir.FullName).FullName;

            TextInfo textInfo = new CultureInfo("es-ES").TextInfo;
            string carpetaDestino = $"{textInfo.ToTitleCase(CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(dtFechaFichero.Month))} {dtFechaFichero.Year}";

            return $"{carpetaOrigen}\\{carpetaDestino}";
        }

        public Dictionary<string, List<DateTime>> GetReportsToMail()
        {
            return _reportsToMail;
        }

        /// <summary>
        /// Ejecuta procedimiento almacenado para generar los cubos E2E (fases) de las Suscripciones activas y de las suscripciones con facturas asociadas.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="sp_controlE2E">Procedimiento almacenado</param>        
        public void ExecuteE2E(Database database, string sp_controlE2E)
        {
            database.Execute(sp_controlE2E);
        }
    }
}
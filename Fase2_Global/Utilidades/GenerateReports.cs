using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Fase2_Global.Utilidades
{
    public class GenerateReports
    {
        private IConfiguration _configuration;
        private ILogger _logger;
        private readonly Database _database;
        private readonly Email _email;

        public GenerateReports(IConfiguration configuration, ILogger<GenerateReports> logger, Database database, Email email)
        {
            this._configuration = configuration;
            this._logger = logger;
            this._database = database;
            this._email = email;
        }

        public void ReportConsultasBI(string date)
        {
            _logger.LogInformation("Generando informe de consultas BI");
            string queryInfSuscripcionesPend = ReadScript("csvBI.sql");
            if (queryInfSuscripcionesPend != null && queryInfSuscripcionesPend != "")
            {
                var msInfSuscripcionesPend = Execute(queryInfSuscripcionesPend, "Suscripciones Fase 2");
                Upload(700, $"InformeConsultasBI_{date}.csv", msInfSuscripcionesPend);
            }
        }

        #region Para ejecutar todos los reportes
        public void GenerateReport(string date)
        {
            _logger.LogInformation("Generando informes...");
            // Suscripciones Pendientes
            string queryInfSuscripcionesPend = ReadScript("Suscripciones_Pendientes.sql");
            if (queryInfSuscripcionesPend != null && queryInfSuscripcionesPend != "")
            {
                var msInfSuscripcionesPend = Execute(queryInfSuscripcionesPend, "Suscripciones Pendientes");
                Upload(306, $"SuscripcionesPendientes_{date}.csv", msInfSuscripcionesPend);
            }

            // Rechazos
            string queryInfRechazos = ReadScript("Rechazos.sql");
            if (queryInfRechazos != null && queryInfRechazos != "")
            {
                var msInfRechazos = Execute(queryInfRechazos, "Rechazos");
                Upload(307, $"Rechazos_{date}.csv", msInfRechazos);
            }

            // Suscripciones Bloqueadas
            string queryInfSuscripcionesBloqueadas = ReadScript("Suscripciones_Bloqueadas.sql");
            if (queryInfSuscripcionesBloqueadas != null && queryInfSuscripcionesBloqueadas != "")
            {
                var msInfSuscripcionesBloqueadas = Execute(queryInfSuscripcionesBloqueadas, "Suscripciones Bloqueadas");
                Upload(308, $"SuscripcionesBloqueadas_{date}.csv", msInfSuscripcionesBloqueadas);
            }

            // Facturado Periodo
            string queryInfFactPeriodo = ReadScript("Factura_periodo.sql");
            if (queryInfFactPeriodo != null && queryInfFactPeriodo != "")
            {
                var msInfFactPeriodo = Execute(queryInfFactPeriodo, "Facturado Periodo");
                Upload(309, $"FacturadoPeriodo_{date}.csv", msInfFactPeriodo);
            }
        }
        #endregion

        private string ReadScript(string filename)
        {
            try
            {
                using (FileStream fs = File.Open(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, filename), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (BufferedStream bs = new BufferedStream(fs))
                    {
                        using (StreamReader sr = new StreamReader(bs, Encoding.GetEncoding("iso-8859-1")))
                        {
                            return sr.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError($"Error: {e.Message}");
#if !DEBUG
                _email.Send(e);
#endif
                return null;
            }
        }

        private void Upload(int reportId, string fileName, MemoryStream stream)
        {
            string strId = Guid.NewGuid().ToString();
            string userId = "8bbc8ba9-deb7-4ce0-8781-83da0a94d791"; // sistemasbi@servinform.es            
            string ExecutingBy = "8bbc8ba9-deb7-4ce0-8781-83da0a94d791"; // sistemasbi@servinform.es
            string url = $@"https://consultasbi.servinform.es/Report/Download/{strId}";
            int ReportResultId;
            string queryInsert = $@"INSERT INTO dbo.ReportResult
	                                    (id, ReportId, UserId, Created, StatusId, DeleteDate, FileName, AppId, Email, Url, ExecutingBy)
                                    VALUES
	                                    ('{strId}', {reportId}, '{userId}', getdate(), 1, dateadd(day, 7, getdate()), '{fileName}', 8, 0, '{url}', '{ExecutingBy}')";

            _logger.LogInformation($"Insertando reporte: {fileName}");

            var connectionString = _configuration.GetConnectionString("ConsultasBI");
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        //var compressBytes = Compress(new MemoryStream(uncompressBytes));
                        queryInsert += ";select SCOPE_IDENTITY()";
                        var command = new SqlCommand(queryInsert, connection, transaction);
                        command.CommandTimeout = 5600;
                        ReportResultId = (int)(decimal)command.ExecuteScalar();

                        string queryUpdate = "UPDATE ReportResult SET fileimage = @fileimage, StatusId = @StatusId WHERE id = @id";
                        using (SqlCommand cmd = new SqlCommand(queryUpdate, connection, transaction))
                        {
                            cmd.CommandTimeout = 5600;
                            cmd.Parameters.Add("@fileImage", SqlDbType.VarBinary).Value = Compress(stream);
                            cmd.Parameters.AddWithValue("StatusId", 5);
                            cmd.Parameters.AddWithValue("id", strId);

                            cmd.ExecuteNonQuery();
                        }

                        transaction.Commit();
                        _logger.LogInformation("Informe generado con éxito.");
                    }
                    catch (Exception e)
                    {
                        transaction.Rollback();
                        _logger.LogError(e.Message);
#if !DEBUG
                        _email.Send(e);
#endif
                        throw e;
                    }
                }
            }

        }

        private byte[] Compress(MemoryStream input)
        {
            using (var compressedStream = new MemoryStream())
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
            {
                input.CopyTo(zipStream);
                zipStream.Close();
                return compressedStream.ToArray();
            }
        }

        private MemoryStream Execute(string query, string name)
        {
            DataTable template;
            template = _database.GetData(query);

            _logger.LogInformation($"Generando informe: {name}");
            try
            {
                MemoryStream ms = new MemoryStream();
                var sw = new StreamWriter(ms);
                //var sr = new StreamReader(ms);

                foreach (DataColumn column in template.Columns)
                {
                    sw.Write(column.ColumnName.ToString() + ";");
                }
                sw.WriteLine();

                foreach (DataRow row in template.Rows)
                {
                    for (int i = 0; i < row.ItemArray.Length; i++)
                    {
                        string rowText = row.ItemArray[i].ToString();
                        if (rowText.Contains(","))
                        {
                            //TODO: Rompe los numéricos
                            //rowText = rowText.Replace(",", "/");
                        }

                        sw.Write(rowText + ";");
                    }
                    sw.WriteLine();
                }
                sw.Flush();
                ms.Position = 0;
                return ms;
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                _email.Send(e);
                return null;
            }
        }
    }
}

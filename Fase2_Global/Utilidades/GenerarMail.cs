using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Fase2_Global.Utilidades
{
    public class GenerarMail
    {
        private ILogger Logger;

        public GenerarMail(ILogger<GenerarMail> logger)
        {
            this.Logger = logger;
        }

        //public void GenerateReportToMail(string fechaProceso)
        //{
        //    string queryDescartados = $"SELECT 'Duplicadas','INVOICE', d.[COD.FACT. ZUORA], d.sysmodified,d.[IMPOR. FAC] FROM duplicadas d WHERE d.sysmodified = '{fechaProceso}' and d.MismoDia = 0";
        //    queryDescartados += $" UNION SELECT 'Duplicadas mismo día',d.FicheroInvoice, d.[COD.FACT. ZUORA], d.sysmodified,d.[IMPOR. FAC] FROM duplicadas d WHERE d.sysmodified = '{fechaProceso}' and d.MismoDia = 1";
        //    queryDescartados += " UNION SELECT 'Sin entrar Por Invoice','FACTURADAS',fni.[COD.FACT. ZUORA],fni.[F. RECEPCI],fni.[IMPOR. FAC] FROM FACT_NO_INVOICE fni";
        //    queryDescartados += " UNION SELECT 'Sin entrar Por Invoice','DEVUELTAS',fnid.[COD.FACT. ZUORA],fnid.[F. RECEPCI],fnid.[IMPOR. FAC] FROM FACT_NO_INVOICE_DEV fnid";
        //    queryDescartados += " UNION SELECT 'Sin entrar Por Invoice','RECIBIDAS',fnir.[COD.FACT. ZUORA],fnir.[F. RECEPCI],fnir.[IMPOR. FAC] FROM FACT_NO_INVOICE_REC fnir";
        //    queryDescartados += " UNION SELECT 'Diferente Importe','FACTURADAS',fnif.[COD.FACT. ZUORA], fnif.[F. RECEPCI],fnif.[Importe en Fichero] FROM FACT_NO_INVOICE_FDA fnif";
        //    queryDescartados += " UNION SELECT 'Diferente Importe','PENDIENTES',fmi.[COD.FACT. ZUORA], fmi.[F. RECEPCI],fmi.[Importe en Fichero] FROM FALLO_MOD_IMP fmi";
        //    queryDescartados += " UNION SELECT 'Diferente Importe','DEVUELTAS',fmid.[COD.FACT. ZUORA], fmid.[F. RECEPCI],fmid.[Importe en Fichero] FROM FALLO_MOD_IMP_DEV fmid";

        //    queryDescartados += " UNION SELECT 'Factura en estado Facturada o Devuelta','DEVUELTAS',cED.[COD.FACT. ZUORA], cED.[F. RECEPCI],cED.[IMPOR. FAC] FROM FACT_DEV_EST_FAC cED";
        //    queryDescartados += " UNION SELECT 'Factura en estado Facturada o Devuelta','FACTURADAS',cEF.[COD.FACT. ZUORA], cEF.[F. RECEPCI],cEF.[IMP. TOTAL FACTURAS] FROM FACT_FDA_EST_DEV cEF";
        //    queryDescartados += " UNION SELECT 'Factura en estado Facturada o Devuelta','RECIBIDAS',cER.[COD.FACT. ZUORA], cER.[F. RECEPCI],cER.[IMPOR. FAC] FROM FACT_REC_EST_FIN cER";

        //    var msDescartados = ServiceProvider.GetService<Exporter>().Execute(queryDescartados);
        //    msDescartados.Position = 0;
        //    //var a = Etl.ConsultasBI.Client.UploaderReport.Upload(300, msDescartados, $"Descartados_{fechaProceso}.xlsx").Result;
        //    Upload(300, $"Descartados_{fechaProceso}.xlsx", msDescartados);

        //    string queryDistrib = "SELECT f.[SEGMENTO MERCADO], f.fechaemision AS [F. EMISION], f.[F. RECEPCI], COUNT(1) AS CANTIDAD ";
        //    queryDistrib += " FROM Facturas f";
        //    queryDistrib += " GROUP BY f.[SEGMENTO MERCADO], f.fechaemision,  f.[F. RECEPCI]";
        //    queryDistrib += " order BY f.fechaemision";

        //    var msDistribucion = ServiceProvider.GetService<Exporter>().Execute(queryDistrib);
        //    msDistribucion.Position = 0;
        //    //var infDist = Etl.ConsultasBI.Client.UploaderReport.Upload(301, msDistribucion, $"Distribucion de recibidos {fechaProceso}.xlsx").Result;
        //    Upload(301, $"Distribucion de recibidos {fechaProceso}.xlsx", msDistribucion);

        //    ////Informe de facturas duplicadas el mismo día
        //    //var duplis = new StreamReader(Configuration.GetValue<string>("OutputFolder") + $"Fact Duplicadas mismo día {DateTime.Now:yyyyMMdd}.txt");
        //    //if (duplis.ReadLine().Contains("InvoiceNumber"))
        //    //{
        //    //    var infDuplis = Etl.ConsultasBI.Client.UploaderReport.Upload(302, duplis.BaseStream, $"Fact Duplicadas mismo día {fechaProceso}.txt").Result;
        //    //}

        //    //Genera informe con el fichero de conteos
        //    //var excelStream = new StreamReader(Configuration.GetValue<string>("IputConteos") + @"Reporte Conteos No Energetico.xlsx");
        //    //Upload(303, excelStream.BaseStream, $"Estadística carga invoices {fechaProceso}.xlsx").Result;            
        //    byte[] data = File.ReadAllBytes(Configuration.GetValue<string>("IputConteos") + @"Reporte Conteos No Energetico.xlsx");
        //    MemoryStream ms = new MemoryStream(data);
        //    ms.Position = 0;
        //    Upload(303, $"Estadística carga invoices {fechaProceso}.xlsx", ms);
        //}

        //private void Upload(int reportId, string fileName, MemoryStream stream)
        //{
        //    string strId = Guid.NewGuid().ToString();
        //    string userId = "8bbc8ba9-deb7-4ce0-8781-83da0a94d791"; // sistemasbi@servinform.es            
        //    string ExecutingBy = "8bbc8ba9-deb7-4ce0-8781-83da0a94d791"; // sistemasbi@servinform.es
        //    string url = $@"https://consultasbi.servinform.es/Report/Download/{strId}";
        //    int ReportResultId;
        //    string queryInsert = $@"INSERT INTO dbo.ReportResult
	       //                             (id, ReportId, UserId, Created, StatusId, DeleteDate, FileName, AppId, Email, Url, ExecutingBy)
        //                            VALUES
	       //                             ('{strId}', {reportId}, '{userId}', getdate(), 1, dateadd(day, 7, getdate()), '{fileName}', 6, 0, '{url}', '{ExecutingBy}')";

        //    Logger.LogInformation($"Insertando reporte: {fileName}");

        //    var connectionString = Configuration.GetConnectionString("ConsultasBI");
        //    using (var connection = new SqlConnection(connectionString))
        //    {
        //        connection.Open();
        //        using (SqlTransaction transaction = connection.BeginTransaction())
        //        {
        //            try
        //            {
        //                //var compressBytes = Compress(new MemoryStream(uncompressBytes));
        //                queryInsert += ";select SCOPE_IDENTITY()";
        //                var command = new SqlCommand(queryInsert, connection, transaction);
        //                command.CommandTimeout = 5600;
        //                ReportResultId = (int)(decimal)command.ExecuteScalar();

        //                Logger.LogInformation($"Insertando fichero en base de datos con id: {ReportResultId}");

        //                string queryUpdate = "UPDATE ReportResult SET fileimage = @fileimage, StatusId = @StatusId WHERE id = @id";
        //                using (SqlCommand cmd = new SqlCommand(queryUpdate, connection, transaction))
        //                {
        //                    cmd.CommandTimeout = 5600;
        //                    cmd.Parameters.Add("@fileImage", SqlDbType.VarBinary).Value = Compress(stream);
        //                    cmd.Parameters.AddWithValue("StatusId", 5);
        //                    cmd.Parameters.AddWithValue("id", strId);

        //                    cmd.ExecuteNonQuery();
        //                }

        //                transaction.Commit();
        //            }
        //            catch (Exception e)
        //            {
        //                transaction.Rollback();
        //                Logger.LogError(e.Message);
        //                throw e;
        //            }
        //        }
        //    }
        //}

        //static public byte[] Compress(Stream input)
        //{
        //    using (var compressedStream = new MemoryStream())
        //    using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
        //    {
        //        input.CopyTo(zipStream);
        //        zipStream.Close();
        //        return compressedStream.ToArray();
        //    }
        //}

    }
}

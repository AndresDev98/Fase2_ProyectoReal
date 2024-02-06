using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using static System.Net.WebRequestMethods;

namespace Fase2_Global.Utilidades
{
    public class EnviarEmail
    {
        private IConfiguration Configuration;
        private ILogger<EnviarEmail> Logger;
        private readonly Database database;

        public EnviarEmail(ILogger<EnviarEmail> logger, IConfiguration configuration, Database database)
        {
            this.Logger = logger;
            this.Configuration = configuration;
            this.database = database;
        }

        internal void Execute()
        {
            string template, body;
            string title = $"Origenes Importados Fase 2. Resumen {DateTime.Now.Date.ToString("dd-MM-yyyy")}";

                Logger.LogInformation("Enviando por email información de la finalización de la carga.");
                
                var prueba = database.GetData(CompruebaData());
                var filesOk = database.GetData(CountFiles());

                template = LoadPathTemplate(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Configuration.GetValue<string>("TemplateEmailSummary")));
                body = LoadTemplate(template, prueba, filesOk);

                Send(title, body, false);

        }

        private string QueryTemplate()
        {

            return @"SELECT NombreFichero, FechaProcesado
                        FROM FICHEROS_PROCESADOS
                        WHERE CAST (FICHEROS_PROCESADOS.FechaProcesado AS DATE) = CAST(GETDATE() -1 AS DATE)";
        }

        private string LoadPathTemplate(string template)
        {
            // Verifica que el archivo de la plantilla exista
            if (!System.IO.File.Exists(template))
                throw new FileNotFoundException("No se pudo encontrar el archivo de la plantilla.", template);

            // Lee el contenido del archivo de la plantilla
            return System.IO.File.ReadAllText(template); ;
        }

        private string LoadTemplate(string template, DataTable data, DataTable data2)
        {

            string tablaHtmlError;
            // Crea una tabla HTML a partir de los datos de DataTable
            string tablaHtml = BuildHtmlTable(data);


            tablaHtmlError = BuildHtmlTable2(data2);
            //template = template.Replace("[COUNT_ORIGEN]", tablaHtmlError);

            template = template.Replace("[COUNT_ORIGEN]", $"{data.Rows.Count.ToString()}");

            template = template.Replace("[IMPORTACIONES_ORIGEN]", QueryTemplate());

            return template.Replace(QueryTemplate(), tablaHtml);            
        }
        
        public string BuildHtmlTable(DataTable data)
        {
            StringBuilder tablaHtml = new StringBuilder();

            // Construir la etiqueta <table>
            tablaHtml.Append(@"<table ALIGN='left' style='border-collapse: collapse; margin: left; border-radius: 10px; box-shadow: 0px 5px 10px rgba(0, 0, 0, 0.3); '>");

            // Construir la fila de encabezado
            tablaHtml.Append("<tr>");
            foreach (DataColumn columna in data.Columns)
            {
                tablaHtml.AppendFormat("<th style='border: 1px solid black; padding: 5px;'>{0}</th>", columna.ColumnName);
            }
            tablaHtml.Append("</tr>");

            // Construir las filas de datos
            foreach (DataRow fila in data.Rows)
            {
                tablaHtml.Append("<tr>");
                foreach (DataColumn columna in data.Columns)
                {
                    tablaHtml.AppendFormat("<th style='border: 1px solid black; padding: 5px;'>{0}</th>", fila[columna.ColumnName]);
                }
                tablaHtml.Append("</tr>");
            }

            // Cerrar la etiqueta </table>
            tablaHtml.Append("</table>");
            tablaHtml.Append("</br> </br>	</br>	</br>	</br>	</br> </br>	</br>	</br>	</br>	</br>	</br>	</br>	</br>	</br>	</br>Un saludo.");
            return tablaHtml.ToString();
        }

        public string BuildHtmlTable2(DataTable data)
        {
            StringBuilder tablaHtml = new StringBuilder(); 
            foreach (DataRow fila in data.Rows)
            {
                foreach (DataColumn columna in data.Columns)
                {
                    //tablaHtml.AppendFormat("<td style='border: 1px solid black; padding: 5px; color:red;'>{0}</td>", fila[columna.ColumnName]);
                    tablaHtml.AppendFormat("&nbsp <FONT FACE='impact' SIZE=4><a style='color:blue;'>  </a></font> &nbsp", fila[columna.ColumnName]);
                }
            } 

            return tablaHtml.ToString();
        }

        public string BuildHtmlTable3(DataTable data)
        {
            StringBuilder tablaHtml = new StringBuilder();
            foreach (DataRow fila in data.Rows)
            {
                foreach (DataColumn columna in data.Columns)
                {
                    //tablaHtml.AppendFormat("<td style='border: 1px solid black; padding: 5px; color:red;'>{0}</td>", fila[columna.ColumnName]);
                    tablaHtml.AppendFormat("&nbsp <FONT FACE='impact' SIZE=4><a style='color:blue;'> FASE 2 </a></font> &nbsp", fila[columna.ColumnName]);
                }
            }

            return tablaHtml.ToString();
        }

        private string CompruebaData()
        {
            return @"SELECT NombreFichero, FechaProcesado
                        FROM FICHEROS_PROCESADOS
                        WHERE CAST (FICHEROS_PROCESADOS.FechaProcesado AS DATE) = CAST(GETDATE() AS DATE)";
        }

        private string CountFiles()
        {
            return $@"SELECT COUNT(NombreFichero) FROM FICHEROS_PROCESADOS
                        WHERE CAST (FICHEROS_PROCESADOS.FechaProcesado AS DATE) = CAST(GETDATE() AS DATE)";
        }

        public void Send(string title, string body, bool isInternal)
        {
            string smtp = isInternal ? "SMTP_Interno" : "SMTP";

            try
            {
                SmtpClient client = new SmtpClient(Configuration.GetSection(smtp).GetValue<string>("host"), Configuration.GetSection(smtp).GetValue<int>("port"));
                MailMessage message;

                client.EnableSsl = true;
                client.Credentials = new NetworkCredential(Configuration.GetSection(smtp).GetValue<string>("username"), Configuration.GetSection(smtp).GetValue<string>("password"));
                message = new MailMessage();

                message.From = new MailAddress(Configuration.GetSection(smtp).GetValue<string>("from"));
                foreach (var to in Configuration.GetSection(smtp).GetValue<string>("to").Split(';').ToList())
                    message.Bcc.Add(to);
                
                message.Body = body;
                message.IsBodyHtml = true;
                message.Subject = title;

                client.Send(message);
            }
            catch (Exception e)
            {
                Logger.LogError($"Error al enviar correo: {e.Message}");
            }
        }
    }
}
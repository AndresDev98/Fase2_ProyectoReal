using Fase2_Global.Importadores;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fase2_Global.Utilidades
{
    internal class Reportes
    {

        private const int BUFFER = 50000;
        private IConfiguration Configuration;
        private Configurator Configurator;
        private Database Database;
        private int lasCode = 0;
        private ILogger<Importador> Logger;
        private IServiceProvider ServiceProvider;

        public Reportes(ILogger<Importador> logger, Configurator configurator, IServiceProvider serviceProvider, IConfiguration configuration, Database database)
        {
            this.Database = database;
            this.Logger = logger;
            this.Configurator = configurator;
            this.ServiceProvider = serviceProvider;
            this.Configuration = configuration;
        }

        public void Execute()
        {
            DateTime fecha = DateTime.Now;

            var reportFolder = Configuration.GetValue<string>("ReportFolder");
            var outputFolder = $"{fecha.ToString("yyyyMMdd_HHmm")}";
            //var outputFolder = $"20220127_1539";
            reportFolder = Path.Combine(reportFolder, outputFolder);
            if (!Directory.Exists(reportFolder))
            {
                Logger.LogInformation($"Creando carpeta {reportFolder}");
                Directory.CreateDirectory(reportFolder);
            }

            

            Execute($"{reportFolder}\\SuscripcionesQUERY {fecha.ToString("ddMMyyyy")}.csv", QuerySuscripciones(), "Suscripciones Querys");


        }

        private string QuerySuscripciones()
        {

            return @"SELECT  
                    num_susc,
                    Id_susc,
                    Estado_susc,
                    Plazo_susc,
                    Renov_susc,
                    Autorenew,
                    PorSeparado,
                    Producto,
                    F_activacion_susc,
                    F_inicio_susc,
                    F_creacion_susc,
                    F_cancelacion_susc,
                    Numero_cuen,
                    Id_cuen,
                    F_creacion_cuen,
                    Batch,
                    Estado_cuen,
                    OtherPayment,
                    IsCommodity,
                    Pasarela,
                    Susc_version,
                    Id_created,
                    type_user_created,
                    CASE WHEN prob_emob = 1 THEN 'Sí' ELSE 'No' END AS prob_emob
                    ,CASE WHEN unico = 1 THEN 'Sí' ELSE 'No' END AS unico
                    ,CASE WHEN crea_usuario = 1 THEN 'Sí' ELSE 'No' END AS crea_usuario
                    ,CASE WHEN recurr = 1 THEN 'Sí' ELSE 'No' END AS recurr
                    ,dif_num_dia
                    ,Motivo
                    , Facturado 
                    ,Amount
                FROM dbo.IN_PENDIENTES_FACTURAR";
        }

        private void Execute(string fileName, string query, string name)
        {
            string line;
            DataTable data;

            try
            {
                Logger.LogInformation($"Ejecutando informe {name}");

                Logger.LogInformation($"Realizando consulta para el informe {name}");

                data = Database.GetData(query);

                if (data.Rows.Count > 0)
                {
                    Logger.LogInformation($"Escribiendo {name} en {fileName}");

                    if (File.Exists(fileName))
                        File.Delete(fileName);

                    using (var writer = new StreamWriter(File.Open(fileName, FileMode.CreateNew), Encoding.GetEncoding("iso-8859-1")))
                    {
                        line = "";
                        for (int i = 0; i < data.Columns.Count; i++)
                        {
                            if (line != "") line += ";";
                            line += data.Columns[i].ColumnName;
                        }
                        writer.WriteLine(line);

                        for (int r = 0; r < data.Rows.Count; r++)
                        {
                            line = "";
                            for (int i = 0; i < data.Columns.Count; i++)
                            {
                                var value = data.Rows[r][i];
                                if (line != "") line += ";";

                                if (value == DBNull.Value)
                                    line += "";
                                else
                                {
                                    switch (data.Columns[i].DataType.Name)
                                    {
                                        case "DateTime":
                                            line += ((DateTime)value).ToString("yyyy-MM-dd");
                                            break;

                                        case "Double":
                                            line += ((double)value).ToString("0.00");
                                            break;

                                        case "Decimal":
                                            line += ((decimal)value).ToString("0.00");
                                            break;

                                        default:
                                            line += data.Rows[r][i].ToString();
                                            break;
                                    }
                                }
                            }
                            writer.WriteLine(line);
                        }
                    }

                    Logger.LogInformation($"Comprimiendo fichero");

                    Logger.LogInformation($"Escritura finalizada ({name})");
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
            }
        }
    }

    

}

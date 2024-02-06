using Fase2_Global.Importadores;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace Fase2_Global.Utilidades
{
    public class Command
    {
        public string Logger { get; set; }

        public string SQL { get; set; }

        public Command()
        {
            SQL = "";
        }
    }

    public class ProcedureManager
    {
        protected IConfiguration Configuration;
        protected ILogger Logger;
        
        private Email Email;

        public ProcedureManager(ILogger<Importador> logger)
        {
            this.Logger = logger;
        }
        internal void ExecuteProcedure(Database database, string procedureName)
        {
            database.Execute(procedureName);
        }

        public void ExecuteProcedures(Database database, string filename)
        {
            ExecuteProcedures(database, filename, new Dictionary<string, string>());
        }

        public void ExecuteProcedures(Database database, string filename, Dictionary<string, string> parameters)
        {
            try
            {
                var commandos = new List<Command>();
                using (FileStream fs = File.Open(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, filename), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (BufferedStream bs = new BufferedStream(fs))
                    {
                        using (StreamReader sr = new StreamReader(bs, Encoding.GetEncoding("iso-8859-1")))
                        {
                            var line = sr.ReadLine();
                            while (line != null)
                            {
                                while (line != null && IsCommand(line) == false)
                                    line = sr.ReadLine();

                                if (line != null)
                                {
                                    var command = new Command();
                                    command.Logger = line;

                                    line = sr.ReadLine();
                                    while (line != null && IsCommand(line) == false)
                                    {
                                        command.SQL += $"{line}{Environment.NewLine}";
                                        line = sr.ReadLine();
                                    }

                                    if (command.SQL != "")
                                    {
                                        commandos.Add(command);
                                    }
                                }
                            }
                        }
                    }
                }

                //var @FechaProceso = $"'{DateTime.Now.ToString("yyyyMMdd")}'";

                foreach (var command in commandos)
                {
                    Logger.LogInformation(command.Logger);

                    foreach (var key in parameters.Keys)
                        command.SQL = command.SQL.Replace(key, parameters[key]);

                    switch (command.Logger.Substring(0, 5))
                    {
                        case "--V--":
                            {
                                var value = (int)database.GetData(command.SQL).Rows[0][0];
                                if (value != 0)
                                    throw new Exception("No se cumple condición");
                            }
                            break;

                        case "--E--":
                            {
                                try
                                {
                                    database.Execute(command.SQL);
                                }
                                catch (Exception e)
                                {
                                    Logger.LogError($"Error al ejecutar: {command.SQL}");
                                    Logger.LogError(e.Message);
                                    throw e;
                                }
                            }
                            break;

                        case "--I--":
                            {
                                // Email.Execute("Prueba de Email: Finaliza la carga " + DateTime.Now.ToString("yyyyMMdd"));
                                //var data = database.GetData(command.SQL);
                                //if (data.Rows.Count > 0)
                                //{
                                //var outputRoot = Configuration.GetValue<string>("OutputEmail");
                                //var outputEmail = Path.Combine(outputRoot, DateTime.Now.ToString("yyyyMMdd"));
                                //var reportFileName = $"{outputEmail}\\{command.Logger.Substring(5, command.Logger.Length - 5).Trim()}-{parameters["@fechaproceso"]}.xlsx";
                                //
                                //    if (Directory.Exists(Path.GetDirectoryName(reportFileName)) == false)
                                //        Directory.CreateDirectory(Path.GetDirectoryName(reportFileName));

                                //    using (ExcelPackage excel = new ExcelPackage(new FileInfo(reportFileName)))
                                //    {
                                //        var ws = excel.Workbook.Worksheets.Add("Datos");
                                //        ws.Cells[1, 1].LoadFromDataTable(data, true);
                                //        excel.Save();
                                //    }
                                //}
                            }
                            break;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
                throw e;
            }
        }
        
        private bool IsCommand(string line) => line.StartsWith("--E--") || line.StartsWith("--V--") || line.StartsWith("--I--");
    }
}
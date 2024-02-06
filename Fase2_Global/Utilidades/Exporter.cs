using Fase2_Global.Importadores;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace Fase2_Global.Utilidades
{
    public class Exporter
    {
        private readonly Database Database;
        private ILogger Logger;

        public Exporter(ILogger<Importador> logger, Database database)
        {
            this.Database = database;
            this.Logger = logger;
        }

        public MemoryStream Execute(string query)
        {
            DataTable template;

            //if (File.Exists(fileName)) File.Delete(fileName);
            Logger.LogInformation($"Generando informe {query}");

            template = Database.GetData(query);
            try
            {
                using (var excel = new ExcelPackage())
                {
                    excel.Workbook.Worksheets.Add("Informe");
                    var ws = excel.Workbook.Worksheets["Informe"];
                    ws.Cells.LoadFromDataTable(template, true);

                    for (var c = 0; c < template.Columns.Count; c++)
                    {
                        if (template.Columns[c].DataType == typeof(DateTime))
                            ws.Column(c + 1).Style.Numberformat.Format = "dd-MM-yyyy";
                    }

                    if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                    {
                        // Esto no funciona en Linux
                        ws.Cells.AutoFitColumns();
                    }

                    MemoryStream outputFile = new MemoryStream();
                    excel.SaveAs(outputFile);
                    return outputFile;
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e.Message);
                return null;
            }
        }
    }
}

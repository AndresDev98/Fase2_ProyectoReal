using Microsoft.Extensions.Logging;

using OfficeOpenXml;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Fase2_Global
{
    public class Configurator
    {
        public Dictionary<string, List<Field>> Registros = new Dictionary<string, List<Field>>();

        //private Database Database;
        private ILogger<Configurator> Logger;

        public Configurator(ILogger<Configurator> logger)
        {
            //  this.Database = database;
            this.Logger = logger;
        }

        public string CreateTable(string code, string tableName)
        {
            StringBuilder script = new StringBuilder();

            script.Append($"create table {tableName} ({Environment.NewLine}");

            script.Append($"  {tableName}Id int not null identity,{Environment.NewLine}");
            script.Append($"  TempId int not null,{Environment.NewLine}");
            script.Append($"  FileId int not null ,{Environment.NewLine}");
            script.Append($"  ParentId int,{Environment.NewLine}");
            script.Append($"  TempParentId int,{Environment.NewLine}");

            foreach (var f in Registros[code])
            {
                switch (f.Type)
                {
                    case FieldType.Text:
                        script.Append($"  [{f.Name}] nvarchar({f.Length}), {Environment.NewLine}");
                        break;

                    case FieldType.Integer:
                        script.Append($"  [{f.Name}] int, {Environment.NewLine}");
                        break;

                    case FieldType.Decimal:
                    case FieldType.DecimalEnString:
                        //script.Append($"  [{f.Name}] numeric({f.Length + f.Decimal},{f.Decimal}), {Environment.NewLine}");
                        script.Append($"  [{f.Name}] real, {Environment.NewLine}");
                        break;

                    case FieldType.Date:
                    case FieldType.DateTime:
                        script.Append($"  [{f.Name}] datetime, {Environment.NewLine}");
                        break;
                }
            }

            script.Append($"constraint {tableName}PK primary key({tableName}Id)");
            script.Append($" )");

            Logger.LogInformation(script.ToString());

            return script.ToString();
        }

        public void Execute()
        {
            GetFields();

            //try
            //{
            //    Database.Execute("drop table movimiento; drop table factura; drop table medida; drop table otrafactura; drop table concepto; drop table conceptoML");
            //}
            //catch (Exception)
            //{ }

            //Database.Execute(CreateTable("00", "movimiento"));
            //Database.Execute(CreateTable("10", "factura"));
            //Database.Execute(CreateTable("15", "medida"));
            //Database.Execute(CreateTable("25", "otrafactura"));
            //Database.Execute(CreateTable("30", "concepto"));
            //Database.Execute(CreateTable("35", "conceptoML"));
            //Database.Execute(CreateTable("51", "InstalacionGenAutoconsumo"));
            //Database.Execute(CreateTable("52", "EnergiaExcedentaria"));
        }

        public void GetFields()
        {
            var fileConfigurator = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "configuracion", "estructura.xlsx");

            using (ExcelPackage excel = new ExcelPackage(new FileInfo(fileConfigurator)))
            {
                var ws = excel.Workbook.Worksheets["estructura"];
                for (int row = 2; row <= ws.Dimension.Rows; row++)
                {
                    var field = new Field
                    {
                        Code = ws.Cells[row, 1].Value.ToString(),
                        Name = (string)ws.Cells[row, 2].Value,
                        Length = (int)(double)ws.Cells[row, 4].Value,
                        Decimal = (ws.Cells[row, 5].Value == null ? 0 : (int)(double)ws.Cells[row, 5].Value),
                        Required = (string)ws.Cells[row, 6].Value == "S",
                        Notes = ws.Cells[row, 9].Value == null ? "" : ws.Cells[row, 9].Value.ToString(),
                    };

                    while (field.Name.IndexOf("  ") > -1)
                        field.Name = field.Name.Replace("  ", " ");

                    if (ws.Cells[row, 3].Value.ToString() == "DecimalAlfanum")
                        field.Type = FieldType.DecimalEnString;
                    else if (ws.Cells[row, 3].Value.ToString() == "Alfanum" && field.Notes == "AAAA-MM-DD")
                        field.Type = FieldType.Date;
                    else if (ws.Cells[row, 3].Value.ToString() == "Alfanum" && field.Notes == "AAAA/MM/DD HH MM SS")
                        field.Type = FieldType.DateTime;
                    else if (ws.Cells[row, 3].Value.ToString() == "Numérico" && field.Notes == "AAAA-MM-DDTHH:MM:SS")
                        field.Type = FieldType.DateTime;
                    else if (ws.Cells[row, 3].Value.ToString() == "Numérico" && field.Decimal != 0)
                        field.Type = FieldType.Decimal;
                    else if (ws.Cells[row, 3].Value.ToString() == "Numérico" && field.Decimal == 0)
                        field.Type = FieldType.Integer;
                    else
                        field.Type = FieldType.Text;

                    if (Registros.ContainsKey(field.Code) == false)
                        Registros.Add(field.Code, new List<Field>());
                    Registros[field.Code].Add(field);
                }
            }
        }
    }
}
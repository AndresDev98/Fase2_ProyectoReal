using Fase2_Global.Utilidades;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Fase2_Global.Importadores
{
    public class ImportadorPURLSemanal : Importador
    {
        public ImportadorPURLSemanal(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 999;

        protected override string GetFicheroTipo() => "PURL";

        protected override string GetHeader() => "Cuenta: XC_Segment;Factura: Fecha de la factura;Factura: Número de factura;Factura: Rate Plan Name;Cuenta: Nombre;Factura: Importe;Factura: PURL;Factura: ConvergentBillingOperations;Factura: XC_CommodityStatus;Cuenta: Notas;Cuenta: Is Commodity Customer";

        protected override string GetTableName() => "IN_PURL";
        protected override string GetTableName2() => "";

        public void ExecutePURLSemanal(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
        {
            var fileName = Path.GetFileName(fullName);
            var date = DateTime.Now; // todo: calcular la fecha a partir del fichero
            var indexes = new Dictionary<string, int>();
            var row = 1;
            var bufferSize = GetBufferSize() - 1;

            using (FileStream fs = File.Open(fullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (BufferedStream bs = new BufferedStream(fs))
                {
                    using (StreamReader sr = new StreamReader(bs, Encoding.GetEncoding("iso-8859-1")))
                    {
                        if (Transaction)
                            database.BeginTransaction();
                        try
                        {
                            var datatableTemplate = database.GetData($"select top 0 * from {GetTableName()}");
                            var datatable = datatableTemplate.Clone();

                            //Insertamos en FICHEROS_PROCESADOS
                            var ficheroId = database.ExecuteInsert($@"
                                INSERT INTO dbo.FICHEROS_PROCESADOS(NombreFichero,FechaProcesado)
                                VALUES('{fileName}',getdate()); select SCOPE_IDENTITY();");

                            //Insertamos en IN_FILEDATE el fichero procesado y fecha
                            //database.Execute($@"
                            //      INSERT INTO dbo.IN_FILEDATE(filedate,idFichero,Fichero)
                            //      VALUES('{fechaFichero}','{GetFicheroTipoId()}','{GetFicheroTipo()}')");

                            // Obtiene los indices de las distintas columnas de la cabecera
                            var line = GetHeader();
                            var fields = ObtieneCamposLinea(line, GetDelimited());

                            for (int t = 0; t < fields.Length; t++)
                                indexes.Add(fields[t].Trim(), t);

                            //Obtiene cabecera del fichero para comparar.
                            var cabeceraFichero = sr.ReadLine();

                            //Comprobamos que la cabecera de las columnas es válida
                            if (!line.Equals(cabeceraFichero)) throw new Exception($"Carga Cancelada. El fichero '{ fileName }' no tiene las columnas correctas.");

                            line = sr.ReadLine();
                            while (line != null && line.Trim() != "")
                            {
                                row++;
                                fields = ObtieneCamposLinea(line, GetDelimited());
                                if (fields.Length > 1)
                                {
                                    var array = new object[datatable.Columns.Count];
                                    var index = 999;
                                    var value = "";

                                    for (int i = 0; i < datatable.Columns.Count; i++)
                                    {
                                        try
                                        {
                                            #region Lectura Columnas Fichero
                                            //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                            switch (datatable.Columns[i].ColumnName)
                                            {
                                                case "idSegmento":
                                                    index = 0;
                                                    value = fields[index].ToUpper();
                                                    switch (value)
                                                    {
                                                        case "B2C":
                                                            value = "2";
                                                            break;
                                                        case "B2B":
                                                            value = "1";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "InvoiceDate":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 2;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 3;
                                                    value = fields[index];
                                                    break;
                                                case "AccountName":
                                                    index = 4;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAmount":
                                                    index = 5;
                                                    value = fields[index];
                                                    break;
                                                case "InvoicePURL":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;
                                                case "idPURL":
                                                    index = 6;
                                                    value = fields[index];
                                                    if (string.IsNullOrEmpty(value))
                                                    {
                                                        value = "0";
                                                    }
                                                    else
                                                    {
                                                        value="1";
                                                    }
                                                    break;
                                                case "ConvergentBillingOperations":
                                                    index = 7;
                                                    value = fields[index];
                                                    //if (value.Length > 5) value = fields[index].Substring(1, 4);
                                                    break;
                                                case "XC_CommodityStatus":
                                                    index = 8;
                                                    value = fields[index];
                                                    break;
                                                case "Notas":
                                                    index = 9;
                                                    value = fields[index];
                                                    break;
                                                case "IsCommodityCustomer":
                                                    index = 10;
                                                    value = fields[index];
                                                    break;
                                                case "idCommodityCostumer":
                                                    index = 10;
                                                    value = fields[index].ToUpper();
                                                    switch (value)
                                                    {
                                                        case "YES":
                                                            value = "1";
                                                            break;
                                                        case "NO":
                                                            value = "2";
                                                            break;
                                                        default:
                                                            value = "999";
                                                            break;
                                                    }
                                                    break;

                                                default:
                                                    index = 999;
                                                    value = "";
                                                    break;
                                            }

                                            #endregion
                                            #region Conversión Dato
                                            if (value == "")
                                                array[i] = DBNull.Value;
                                            else
                                            {

                                                switch (datatable.Columns[i].DataType.ToString())
                                                {
                                                    case "System.DateTime":
                                                        {
                                                            array[i] = ConvierteDatoFecha(value);
                                                        }
                                                        break;

                                                    case "System.String":
                                                        array[i] = value;
                                                        break;

                                                    case "System.Decimal":
                                                        {
                                                            if (value.StartsWith("-"))
                                                                value = "-" + value.Substring(1, value.Length - 1).Trim();

                                                            value = value.Replace(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                                                            value = value.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

                                                            while (value.Split(',').Count() > 2)
                                                            {
                                                                var regex = new Regex(Regex.Escape(","));
                                                                value = regex.Replace(value, "", 1);
                                                            }

                                                            array[i] = double.Parse(value, NumberStyles.Float, CultureInfo.CurrentCulture);
                                                        }
                                                        break;

                                                    case "System.Int32":
                                                        array[i] = int.Parse(value);
                                                        break;

                                                    default:
                                                        throw new Exception($"Tipo {datatable.Columns[i].DataType.ToString()} no soportado en la lectura");
                                                }
                                            }
                                            #endregion
                                        }
                                        catch (Exception e)
                                        {
                                            throw new Exception($"Error durante la lectura y proceso de datos del fichero '{fileName}' en la linea {row} para el campo(BBDD): {datatable.Columns[i].ColumnName}. ERROR: {e.Message}");
                                        }
                                    }

                                    datatable.Rows.Add(array);
                                    if (datatable.Rows.Count > bufferSize)
                                    {
                                        database.Bulk(datatable, GetTableName());
                                        datatable = datatableTemplate.Clone();
                                    }
                                }
                                line = sr.ReadLine();
                            }

                            if (datatable.Rows.Count > 0)
                                database.Bulk(datatable, GetTableName());

                            if (Transaction)
                                database.CommitTransaction();
                        }
                        catch (Exception e)
                        {
                            if (Transaction)
                                database.RollbackTransaction();
                            throw e;
                        }
                    }
                }
            }
        }
    }
}

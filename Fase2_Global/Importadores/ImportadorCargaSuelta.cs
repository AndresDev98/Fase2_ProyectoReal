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
    public class ImportadorCargaSuelta : Importador
    {
        public ImportadorCargaSuelta(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 0;

        protected override string GetFicheroTipo() => "-";

        protected override string GetHeader() => "";//PERSONALIZAR SEGÚN FICHERO

        protected override string GetTableName() => "TEMP_FACTURAS_OTHERPAYMENTMETHOD";//CAMBIAR A LA NECESARIA
        protected override string GetTableName2() => "";//INCLUIR SI ES NECESARIA

        public List<string> ExecuteCargaSuelta(Database database, string fullName, bool Transaction = false)
        {
            var fileName = Path.GetFileName(fullName);
            var date = DateTime.Now; // todo: calcular la fecha a partir del fichero
            var indexes = new Dictionary<string, int>();
            var row = 1;
            var bufferSize = GetBufferSize() - 1;
            List<string> lineasErroneas = new List<string>();

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

                            // Obtiene los indices de las distintas columnas de la cabecera
                            var line = GetHeader();
                            var fields = ObtieneCamposLinea(line, GetDelimited());
                            var nCampos = fields.Length;
                            var nLineas = File.ReadLines(fullName).Count();

                            for (int t = 0; t < fields.Length; t++)
                                indexes.Add(fields[t].Trim(), t);

                            //Obtiene cabecera del fichero para comparar.
                            var cabeceraFichero = sr.ReadLine();

                            //Comprobamos que la cabecera de las columnas es válida //Comprobamos que tienen ; como separador
                            //if (!line.Equals(cabeceraFichero)) throw new Exception($"Carga Cancelada. El fichero '{ fileName }' no tiene las columnas correctas.");
                            if (!cabeceraFichero.Contains(';')) throw new Exception($"Carga Cancelada. El fichero '{ fileName }' no tiene las columnas correctas.");

                            line = sr.ReadLine();
                            while (line != null && line.Trim() != "" || ((line == null || line.Trim() == "") && row + 1 <= nLineas))
                            {
                                row++;
                                fields = ObtieneCamposLinea(line, GetDelimited());
                                if (fields.Length > 1)
                                {
                                    //Comprobamos que el campo Informacion Adicional tenga los 3 subcampos
                                    var array = new object[datatable.Columns.Count];
                                    var index = 999;
                                    var value = "";

                                    for (int i = 0; i < datatable.Columns.Count; i++)
                                    {
                                        try
                                        {
                                            //MODIFICAR COLUMNAS SEGÚN TABLA BBDD
                                            #region Lectura Columnas Fichero 
                                            //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                            switch (datatable.Columns[i].ColumnName)
                                            {
                                                case "InvoiceNumber":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "idOtherPaymentMethodType":
                                                    index = 21;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "Bank Transfer":
                                                            value = "1";
                                                            break;
                                                        case "Barcode":
                                                            value = "2";
                                                            break;
                                                        case "Commodity Bill":
                                                            value = "3";
                                                            break;
                                                        case "Commodity Customer":
                                                            value = "4";
                                                            break;
                                                        case "External Financing":
                                                            value = "5";
                                                            break;
                                                        case "Sin identificar":
                                                            value = "6";
                                                            break;
                                                        default:
                                                            value = "6";
                                                            break;
                                                    }
                                                    break;
                                                case "ConvergentBillingOperations":
                                                    index = 41;
                                                    value = fields[index];
                                                    //if (value.Length > 5) value = fields[index].Substring(1, 4);
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

            return lineasErroneas;
        }
    }
}

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
    public class ImportadorRechazosSemanalB2B : Importador
    {
        public ImportadorRechazosSemanalB2B(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 16;

        protected override string GetFicheroTipo() => "Rechazos B2B";

        protected override string GetHeader() => "Fecha/Hora de apertura;Antigüedad (Horas);Número del caso;Tipo;Subcaso;Estado;Nombre de la cuenta;Nombre del contacto;Propietario del caso;Asunto;Fecha/Hora de cierre;Modo de Resolución;Resultado;Información adicional;Última modificación de caso realizada por;Id. del caso;Fecha de la última modificación de caso";

        protected override string GetTableName() => "IN_RECHAZOS_B2B";
        protected override string GetTableName2() => "STG_RECHAZOS_HISTORICO";

        public List<string> ExecuteRechazosSemanalB2B(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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
                            var datatableTemplateHistorico = database.GetData($"select top 0 * from {GetTableName2()}");
                            var datatableHistorico = datatableTemplateHistorico.Clone();
                            var dtMaestroRefuseStatus = database.GetData($"SELECT idRefuseStatus, refuseStatus FROM MAESTRO_REFUSE_STATUS");

                            //Insertamos en FICHEROS_PROCESADOS
                            var ficheroId = database.ExecuteInsert($@"
                                INSERT INTO dbo.FICHEROS_PROCESADOS(NombreFichero,FechaProcesado)
                                VALUES('{fileName}',getdate()); select SCOPE_IDENTITY();");

                            //Insertamos en IN_FILEDATE el fichero procesado y fecha
                            database.Execute($@"
                                  INSERT INTO dbo.IN_FILEDATE(filedate,idFichero,Fichero)
                                  VALUES('{fechaFichero}','{GetFicheroTipoId()}','{GetFicheroTipo()}')");

                            // Obtiene los indices de las distintas columnas de la cabecera
                            var line = GetHeader();
                            var fields = ObtieneCamposLinea(line, GetDelimited());
                            var nCampos = fields.Length;
                            var nLineas = File.ReadLines(fullName).Count();

                            for (int t = 0; t < fields.Length; t++)
                                indexes.Add(fields[t].Trim(), t);

                            //Obtiene cabecera del fichero para comparar.
                            var cabeceraFichero = sr.ReadLine();

                            //Comprobamos que la cabecera de las columnas es válida
                            if (!cabeceraFichero.Equals(line))
                            {
                                if (!cabeceraFichero.Equals("\"Fecha/Hora de apertura\";\"Antigüedad (Horas)\";\"Número del caso\";\"Tipo\";\"Subcaso\";\"Estado\";\"Nombre de la cuenta\";\"Nombre del contacto\";\"Propietario del caso\";\"Asunto\";\"Fecha/Hora de cierre\";\"Modo de Resolución\";\"Resultado\";\"Información adicional\";\"Última modificación de caso realizada por\";\"Id. del caso\";\"Fecha de la última modificación de caso\""))
                                {
                                    throw new Exception($"Carga Cancelada. El fichero '{ fileName }' no tiene las columnas correctas.");
                                }                                    
                            }
                            

                            line = sr.ReadLine();
                            while (line != null && line.Trim() != "" || ((line == null || line.Trim() == "") && row+1 <= nLineas))
                            {
                                row++;
                                //Comprobamos si la linea tiene el número de campos correctos. En caso de no ser así, ignoramos la linea
                                if (ObtieneCamposLinea(line, GetDelimited()).Length == nCampos)
                                {
                                    fields = ObtieneCamposLinea(line, GetDelimited());
                                    if (fields.Length > 1)
                                    {
                                        if (fields[13].Split(" - ").Count() == 3)
                                        {  //Comprobamos que el campo Informacion Adicional tenga los 3 subcampos
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
                                                        case "RefuseOpenDate":
                                                            index = 0;
                                                            value = fields[index];
                                                            if(value.Length>=10) value = value.Substring(0,10);
                                                            break;
                                                        case "Antiguedad":
                                                            index = 1;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                value = Math.Truncate(Convert.ToDouble(fields[index])).ToString();
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "NumeroCaso":
                                                            index = 2;
                                                            value = fields[index];
                                                            break;
                                                        case "Tipo":
                                                            index = 3;
                                                            value = fields[index];
                                                            break;
                                                        case "Subcaso":
                                                            index = 4;
                                                            value = fields[index];
                                                            break;
                                                        case "Estado":
                                                            index = 5;
                                                            value = fields[index];
                                                            break;
                                                        case "idRefuseStatus":
                                                            index = 5;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                value = fields[index];
                                                                //Obtenemos el Id Correspondiente al "Estado" a partir del DataTable previamente cargado.
                                                                foreach (DataRow linea in dtMaestroRefuseStatus.Rows)
                                                                {
                                                                    if (linea[1].ToString() == value) value = linea[0].ToString();
                                                                }
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "AccountName":
                                                            index = 6;
                                                            value = fields[index];
                                                            break;
                                                        case "NombreContacto":
                                                            index = 7;
                                                            value = fields[index];
                                                            break;
                                                        case "PropietarioDelCaso":
                                                            index = 8;
                                                            value = fields[index];
                                                            break;
                                                        case "Asunto":
                                                            index = 9;
                                                            value = fields[index];
                                                            break;
                                                        case "FechaCierre":
                                                            index = 10;
                                                            value = fields[index];
                                                            break;
                                                        case "ModoResolucion":
                                                            index = 11;
                                                            value = fields[index];
                                                            break;
                                                        case "Resultado":
                                                            index = 12;
                                                            value = fields[index];
                                                            break;
                                                        case "InvoiceNumber":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) value = fields[index].Split(" - ")[0];
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "idRefuseErrorDescription":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) value = fields[index].Split(" - ")[1];
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "ErrorDescription":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) value = fields[index].Split(" - ")[2];
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        //case "InformacionCierre":
                                                        //    index = 14;
                                                        //    value = fields[index];
                                                        //    break;
                                                        case "UltimaModificacion":
                                                            index = 14;
                                                            value = fields[index];
                                                            break;
                                                        case "FechaModificacion":
                                                            index = 16;
                                                            value = fields[index];
                                                            break;
                                                        case "idRefuse":
                                                            value = "1";//Constante
                                                            break;
                                                        case "idSegmento":
                                                            value = "1";//Constante
                                                            break;
                                                        case "idRefuseIssue":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string ErrorDescription = fields[index].Split(" - ")[2].ToLower();
                                                                if (ErrorDescription.Contains("rechazada"))
                                                                {
                                                                    value = "1";//Constante
                                                                }
                                                                else
                                                                {
                                                                    if (ErrorDescription.Contains("devuelta"))
                                                                    {
                                                                        value = "2";//Constante
                                                                    }
                                                                    else
                                                                    {
                                                                        value = "3";//Constante
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                value = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
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


                                            //RECHAZOS_HISTORICO
                                            var arrayHistorico = new object[datatableHistorico.Columns.Count];
                                            var valueHistorico = "";

                                            for (int i = 0; i < datatableHistorico.Columns.Count; i++)
                                            {
                                                
                                                try
                                                {
                                                    #region Lectura Columnas Fichero
                                                    //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                                    switch (datatableHistorico.Columns[i].ColumnName)
                                                    {
                                                        case "RefuseOpenDate":
                                                            index = 0;
                                                            valueHistorico = fields[index];
                                                            if (valueHistorico.Length >= 10) valueHistorico = valueHistorico.Substring(0, 10);
                                                            break;
                                                        case "Antiguedad":
                                                            index = 1;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                valueHistorico = Math.Truncate(Convert.ToDouble(fields[index])).ToString();
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "NumeroCaso":
                                                            index = 2;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "Subcaso":
                                                            index = 4;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "Estado":
                                                            index = 5;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "idRefuseStatus":
                                                            index = 5;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                valueHistorico = fields[index];
                                                                //Obtenemos el Id Correspondiente al "Estado" a partir del DataTable previamente cargado.
                                                                foreach (DataRow linea in dtMaestroRefuseStatus.Rows)
                                                                {
                                                                    if (linea[1].ToString() == valueHistorico) valueHistorico = linea[0].ToString();
                                                                }
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "AccountName":
                                                            index = 6;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "NombreContacto":
                                                            index = 7;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "PropietarioDelCaso":
                                                            index = 8;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "Asunto":
                                                            index = 9;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "FechaCierre":
                                                            index = 10;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "ModoResolucion":
                                                            index = 11;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "Resultado":
                                                            index = 12;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "Invoice Number":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) valueHistorico = fields[index].Split(" - ")[0];
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "idRefuseErrorDescription":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) valueHistorico = fields[index].Split(" - ")[1];
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "ErrorDescription":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string infoAdicional = fields[index];
                                                                if (!string.IsNullOrWhiteSpace(infoAdicional)) valueHistorico = fields[index].Split(" - ")[2];
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        //case "InformacionCierre":
                                                        //    index = 14;
                                                        //    valueHistorico = fields[index];
                                                        //    break;
                                                        case "UltimaModificacion":
                                                            index = 14;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "FechaUltimaModificacion":
                                                            index = 16;
                                                            valueHistorico = fields[index];
                                                            break;
                                                        case "idRefuse":
                                                            valueHistorico = "1";//Constante
                                                            break;
                                                        case "idSegmento":
                                                            valueHistorico = "1";//Constante
                                                            break;
                                                        case "idRefuseIssue":
                                                            index = 13;
                                                            if (!string.IsNullOrEmpty(fields[index].Trim()))
                                                            {
                                                                string ErrorDescription = fields[index].Split(" - ")[2].ToLower();
                                                                if (ErrorDescription.Contains("rechazada"))
                                                                {
                                                                    valueHistorico = "1";//Constante
                                                                }
                                                                else
                                                                {
                                                                    if (ErrorDescription.Contains("devuelta"))
                                                                    {
                                                                        valueHistorico = "2";//Constante
                                                                    }
                                                                    else
                                                                    {
                                                                        valueHistorico = "3";//Constante
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                valueHistorico = "";
                                                                Logger.LogInformation($"CAMPO VACÍO. Campo: {indexes.FirstOrDefault(x => x.Value == index).Key}. Linea: {row}");
                                                            }
                                                            break;
                                                        case "filedate":
                                                            valueHistorico = fechaFichero.ToString();//Constante
                                                            break;
                                                        default:
                                                            index = 999;
                                                            valueHistorico = "";
                                                            break;

                                                    }
                                                    #endregion
                                                    #region Conversión Datos
                                                    if (valueHistorico == "")
                                                        arrayHistorico[i] = DBNull.Value;
                                                    else
                                                    {

                                                        switch (datatableHistorico.Columns[i].DataType.ToString())
                                                        {
                                                            case "System.DateTime":
                                                                {
                                                                    arrayHistorico[i] = ConvierteDatoFecha(valueHistorico);
                                                                }
                                                                break;

                                                            case "System.String":
                                                                arrayHistorico[i] = valueHistorico;
                                                                break;

                                                            case "System.Decimal":
                                                                {
                                                                    if (valueHistorico.StartsWith("-"))
                                                                        valueHistorico = "-" + valueHistorico.Substring(1, valueHistorico.Length - 1).Trim();

                                                                    valueHistorico = valueHistorico.Replace(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                                                                    valueHistorico = valueHistorico.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

                                                                    if (valueHistorico.Split(',').Count() > 2)
                                                                    {
                                                                        var regex = new Regex(Regex.Escape(","));
                                                                        valueHistorico = regex.Replace(valueHistorico, "", 1);
                                                                    }

                                                                    arrayHistorico[i] = double.Parse(valueHistorico, NumberStyles.Float, CultureInfo.CurrentCulture);
                                                                }
                                                                break;

                                                            case "System.Int32":
                                                                arrayHistorico[i] = int.Parse(valueHistorico);
                                                                break;

                                                            default:
                                                                throw new Exception($"Tipo {datatableHistorico.Columns[i].DataType.ToString()} no soportado en la lectura");
                                                        }
                                                    }
                                                    #endregion
                                                }
                                                catch (Exception e)
                                                {
                                                    throw new Exception($"Error durante la lectura y proceso de datos del fichero '{fileName}' en la linea {row} para el campo(BBDD): {datatableHistorico.Columns[i].ColumnName}. ERROR: {e.Message}");
                                                }

                                            }
                                            datatableHistorico.Rows.Add(arrayHistorico);

                                            if (datatableHistorico.Rows.Count > bufferSize)
                                            {
                                                database.Bulk(datatableHistorico, GetTableName2());
                                                datatableHistorico = datatableTemplateHistorico.Clone();
                                            }
                                        }
                                    }
                                    else
                                    {
                                        //Guardamos la linea en el fichero de errores. Informacion Adicional no tiene los 3 subcampos
                                        lineasErroneas.Add($"Línea: {row} (subcampos Informacion Adicional) --> {line}");
                                        Logger.LogInformation($"Línea {row} ignorada por errores en campo Informacion Adicional.");
                                    }
                                }
                                else
                                {
                                    //Guardamos la linea en el fichero de errores.
                                    lineasErroneas.Add($"Línea: {row} --> {line}");
                                    Logger.LogInformation($"Línea {row} ignorada por errores.");
                                }

                                line = sr.ReadLine();
                            }

                            if (datatable.Rows.Count > 0)
                                database.Bulk(datatable, GetTableName());
                            if (datatableHistorico.Rows.Count > 0)
                                database.Bulk(datatableHistorico, GetTableName2());

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

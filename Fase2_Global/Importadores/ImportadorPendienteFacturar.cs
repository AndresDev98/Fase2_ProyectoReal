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
    public class ImportadorPendienteFacturar : Importador
    {
        public ImportadorPendienteFacturar(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => '\t';

        protected override int GetFicheroTipoId() => 39;

        protected override string GetFicheroTipo() => "Pendientes Facturar";

        protected override string GetHeader() => "Suscripción: Nombre	Suscripción: ID	Suscripción: Estado	Suscripción: Plazo actual	Suscripción: Plazo de renovación	Suscripción: Renovación automática	Suscripción: Separación de factura	Plan de tarifa: Nombre	Suscripción: Fecha de activación del servicio	Suscripción: Fecha de inicio de la suscripción	Suscripción: Fecha de creación original	Suscripción: Fecha de cancelación	Cuenta: Número de cuenta	Cuenta: ID	Cuenta: Fecha de creación	Cuenta: Lote de facturación	Cuenta: Estado	Cuenta: otherPaymentMethodType	Cuenta: Is Commodity Customer	Cuenta: Nombre de pasarela de pago	Suscripción: Versión	Suscripción: ID de creador";

        protected override string GetTableName() => "IN_PENDIENTES_FACTURAR";

        protected override string GetTableName2() => "";

        private Dictionary<string, Dictionary<string, int>> MasterKey;

        public void ExecutePendienteFacturar(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
        {
            var fileName = Path.GetFileName(fullName);
            var date = DateTime.Now; // todo: calcular la fecha a partir del fichero
            var indexes = new Dictionary<string, int>();
            var row = 1;
            var bufferSize = GetBufferSize() - 1;
            MasterKey = new Dictionary<string, Dictionary<string, int>>();

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
                            database.Execute($@"
                                  INSERT INTO dbo.IN_FILEDATE(filedate,idFichero,Fichero)
                                  VALUES('{fechaFichero}','{GetFicheroTipoId()}','{GetFicheroTipo()}')");

                            // Obtiene los indices de las distintas columnas de la cabecera
                            var line = GetHeader();
                            var fields = ObtieneCamposLinea(line, GetDelimited());

                            for (int t = 0; t < fields.Length; t++)
                                indexes.Add(fields[t].Trim(), t);

                            //Obtiene cabecera del fichero para comparar.
                            var cabeceraFichero = sr.ReadLine();

                            //Comprobamos que la cabecera de las columnas es válida
                            if (!line.Equals(cabeceraFichero)) throw new Exception($"Carga Cancelada. El fichero '{fileName}' no tiene las columnas correctas.");

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
                                            switch (datatable.Columns[i].ColumnName.Trim())
                                            {
                                                case "num_susc":
                                                    index = 0;
                                                    value = fields[index];
                                                    break;
                                                case "Id_susc":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "Estado_susc":
                                                    index = 2;
                                                    value = fields[index];
                                                    break;                                                
                                                case "Plazo_susc":
                                                    index = 3;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "Renov_susc":
                                                    index = 4;
                                                    value = fields[index];
                                                    break;
                                                case "Autorenew":
                                                    index = 5;
                                                    value = fields[index];
                                                    break;
                                                case "PorSeparado":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;                                                
                                                case "Producto":
                                                    index = 7;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "F_activacion_susc":
                                                    index = 8;
                                                    value = fields[index];
                                                    break;
                                                case "F_inicio_susc":
                                                    index = 9;
                                                    value = fields[index];
                                                    break;
                                                case "F_creacion_susc":
                                                    index = 10;
                                                    value = fields[index];
                                                    break;                                                
                                                case "F_cancelacion_susc":
                                                    index = 11;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "Numero_cuen":
                                                    index = 12;
                                                    value = fields[index];
                                                    break;
                                                case "Id_cuen":
                                                    index = 13;
                                                    value = fields[index];
                                                    break;
                                                case "F_creacion_cuen":
                                                    index = 14;
                                                    value = fields[index];
                                                    break;                                                
                                                case "Batch":
                                                    index = 15;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "Estado_cuen":
                                                    index = 16;
                                                    value = fields[index];
                                                    break;                                                
                                                case "OtherPayment":
                                                    index = 17;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "IsCommodity":
                                                    index = 18;
                                                    value = fields[index];
                                                    break;
                                                case "Pasarela":
                                                    index = 19;
                                                    value = fields[index];
                                                    break;
                                                case "Susc_version":
                                                    index = 20;
                                                    value = fields[index];
                                                    break;                                                
                                                case "Id_created":
                                                    index = 21;                                                    
                                                    value = fields[index];
                                                    break;
                                                //case "type_user_created ":
                                                //    index = 22;
                                                //    value = fields[index];
                                                //    break;

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
                                                        array[i] = int.Parse(value.Replace(".",""));
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

        private string ValidateImporte(string value)
        {
            string strOut = value;

            if (value.Contains("€"))
                strOut = value.Substring(0, value.Length - 2);

            return strOut;
        }

        private string GetForeignKey(Database data, string values, string tableName, string fieldKeyName)
        {
            Dictionary<string, int> dic;

            // Si no se ha cargado todavía la tabla maestra se carga
            if (MasterKey.ContainsKey(tableName) == false)
            {
                dic = data.GetData($"SELECT {fieldKeyName},Descripcion FROM {tableName}").AsEnumerable().ToDictionary(x => x.Field<string>("Descripcion"), x => x.Field<int>(fieldKeyName));
                MasterKey.Add(tableName, dic);
            }
            else
                dic = MasterKey[tableName];

            // Asigna id tabla maestra
            var value = values.Trim().Replace("'", "''");
            if (dic.ContainsKey(value) == false)
            {
                var id = data.ExecuteInsert($"INSERT INTO {tableName}(Descripcion) VALUES('{value}')");
                dic.Add(value, id);
                return id.ToString();
            }
            else
                return dic[value].ToString();
        }
    }
}

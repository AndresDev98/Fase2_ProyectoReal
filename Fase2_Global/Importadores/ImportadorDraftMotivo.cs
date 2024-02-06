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
    public class ImportadorDraftMotivo : Importador
    {
        public ImportadorDraftMotivo(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 39;

        protected override string GetFicheroTipo() => "Draft B2B";

        protected override string GetHeader() => "Invoice: Invoice Date;Invoice Item: Service Start Date;Invoice Item: Service End Date;Account: XC_CommodityFrequency;Product Rate Plan Charge: Charge Type;Product Rate Plan Charge: Billing Period;Invoice: Invoice Number;Subscription: Name;Invoice: Code;Account: Simplified Invoice;Invoice: Reference To Original Invoice;Invoice: Rate Plan Name;Account: Account Number;Invoice: Due Date;Account: is Guest;Account: Identity Type;Subscription: Everest_Contract_Id;Account: Identity Number;Bill To: State/Province;Account: Name;Invoice: Amount;Invoice: Amount Without Tax;Invoice: Tax Amount;Invoice: Doxee Tax Rate;Invoice: Balance;Account: Currency;Invoice: Status;Invoice: PURL;Account: otherPaymentMethodType;Default Payment Method: Credit Card Type;Default Payment Method: Credit Card Mask Number;Sold To: Work Email;Bill To: First Name;Invoice: Updated Date;Invoice: Includes Usage;Account: Created Date;Account: Legal Entity;Account: EntityAccountNumber;Invoice: Validationerrorcode;Invoice: Created Date;Default Payment Method: Type;Account: Invoice Delivery Preferences Print;Account: Invoice Delivery Preferences Email;Invoice: Validationerrordescription;Journal Entry: Subscription Rate Plan Charge WBE;Invoice: EntityInvoiceNumber;Invoice: Source;Invoice: Source ID;Account: Is Commodity Customer;Rate Plan Charge: Invoice Description;Invoice: ID;Account: ID";

        protected override string GetTableName() => "IN_DRAFTS";

        protected override string GetTableName2() => "";

        private string GetDIMMotivo() => "DIM_DRAFTS_MOTIVO";
        private string GetFieldMotivo() => "idSituacionMotivo";
        //private string GetDIMSituacion() => "DIM_DRAFTS_SITUACION";
        //private string GetFieldSituacion() => "idSituacionSituacion";
        //private string GetDIMObservacion() => "DIM_DRAFTS_OBSERVACION";
        //private string GetFieldObservacion() => "idSituacionObservacion";
        private Dictionary<string, Dictionary<string, int>> MasterKey;

        public void ExecuteDraftMotivos(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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
                                                case "InvoiceDate":
                                                    index = 0;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "SubscriptionName":
                                                    index = 2;
                                                    value = fields[index];
                                                    break;                                                
                                                case "InvoiceCode":
                                                    index = 3;                                                    
                                                    value = fields[index];
                                                    break;
                                                case "SimplifiedInvoice":
                                                    index = 4;
                                                    value = fields[index];
                                                    break;
                                                case "ReferenceToOriginalInvoice":
                                                    index = 5;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;
                                                case "AccountNumber":
                                                    index = 7;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDueDate":
                                                    index = 8;
                                                    value = fields[index];
                                                    break;
                                                case "IsGuest":
                                                    index = 9;
                                                    value = fields[index];
                                                    break;
                                                case "IdentityType":
                                                    index = 10;
                                                    value = fields[index];
                                                    break;
                                                case "Everest_Contract_Id":
                                                    index = 11;
                                                    value = fields[index];
                                                    break;
                                                case "IdentityNumber":
                                                    index = 12;
                                                    value = fields[index];
                                                    break;
                                                case "StateProvince":
                                                    index = 13;
                                                    value = fields[index];
                                                    break;
                                                case "AccountName":
                                                    index = 14;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAmount":
                                                    index = 15;
                                                    value = fields[index];
                                                    //value = ValidateDecimalNumber(fields[index]);
                                                    break;
                                                case "AmountWithoutTax":
                                                    index = 16;
                                                    value = fields[index];
                                                    break;
                                                case "TaxAmount":
                                                    index = 17;
                                                    value = fields[index];
                                                    break;
                                                case "DoxeeTaxRate":
                                                    index = 18;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceBalance":
                                                    index = 19;
                                                    value = fields[index];
                                                    break;
                                                case "Currency":
                                                    index = 20;
                                                    value = fields[index];
                                                    break;
                                                case "InvoicePURL":
                                                    index = 21;
                                                    value = fields[index];
                                                    break;
                                                case "OtherPaymentMethodType":
                                                    index = 22;
                                                    value = fields[index];
                                                    break;
                                                case "CreditCardType":
                                                    index = 23;
                                                    value = fields[index];
                                                    break;
                                                case "CreditCardMaskNumber":
                                                    index = 24;
                                                    value = fields[index];
                                                    break;
                                                case "WorkEmail":
                                                    index = 25;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAddreseName":
                                                    index = 26;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceUpdateDate":
                                                    index = 27;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceIncludesUsage":
                                                    index = 28;
                                                    value = fields[index];
                                                    break;
                                                case "AccountCreationDate":
                                                    index = 29;
                                                    value = fields[index];
                                                    break;
                                                case "AccountLegalEntity":
                                                    index = 30;
                                                    value = fields[index];
                                                    break;
                                                case "EntityAccountNumber":
                                                    index = 31;
                                                    value = fields[index];
                                                    break;
                                                case "ValidationErrorCode":
                                                    index = 32;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceCreationDate":
                                                    index = 33;
                                                    value = fields[index];
                                                    break;
                                                case "DefaultPaymentMethodType":
                                                    index = 34;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDeliveryPreferencesPrint":
                                                    index = 35;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDeliveryPreferencesEmail":
                                                    index = 36;
                                                    value = fields[index];
                                                    break;
                                                case "ValidationErrorDescription":
                                                    index = 37;
                                                    value = fields[index];
                                                    break;
                                                case "SubscriptionRatPlanChargeWBE":
                                                    index = 38;
                                                    value = fields[index];
                                                    break;
                                                case "EntityInvoiceNumber":
                                                    index = 39;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceSource":
                                                    index = 40;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceSourceID":
                                                    index = 41;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDescription":
                                                    index = 42;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceID":
                                                    index = 43;
                                                    value = fields[index];
                                                    break;
                                                case "AccountID":
                                                    index = 44;
                                                    value = fields[index];
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

        private string ValidateDecimalNumber(string value)
        {
            dynamic decimalNumber = value;
            //string strOut = value;
            decimalNumber = Convert.ToDecimal(decimalNumber);

            //if (value.Equals("Yes"))
            //    strOut = "1";
            //else
            //    strOut = "0";

            return decimalNumber;
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

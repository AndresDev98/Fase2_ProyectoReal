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
    public class ImportadorSeguimientoSemanalB2B : Importador
    {
        public ImportadorSeguimientoSemanalB2B(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 23;

        protected override string GetFicheroTipo() => "Seguimiento B2B";

        protected override string GetHeader() => "Factura: Fecha de la factura;Factura: Número de factura;Factura: Code;Cuenta: Simplified Invoice;Factura: Reference To Original Invoice;Factura: Rate Plan Name;Cuenta: Número de cuenta;Factura: Fecha de vencimiento;Cuenta: is Guest;Cuenta: Identity Type;Cuenta: Identity Number;Destinatario de factura: Estado o provincia;Cuenta: Nombre;Factura: Importe;Factura: Importe sin impuestos;Factura: Importe del impuesto;Factura: Doxee Tax Rate;Factura: Saldo;Cuenta: Divisa;Factura: Estado;Factura: PURL;Cuenta: otherPaymentMethodType;Método de pago predeterminado: Tipo de tarjeta de crédito;Método de pago predeterminado: Número de máscara de la tarjeta de crédito;Destinatario de factura: Correo electrónico del trabajo;Destinatario de factura: Nombre;Factura: Fecha de actualización;Factura: Incluye Uso;Cuenta: Fecha de creación;Cuenta: Legal Entity;Cuenta: EntityAccountNumber;Factura: Fecha de creación;Método de pago predeterminado: Tipo;Cuenta: Copia de las preferencias de entrega de la factura;Cuenta: Correo electrónico para las preferencias de entrega de la factura;Cuenta: Día del ciclo de facturación;Factura: Dunning Category;Factura: severityLevel;Factura: Validationerrordescription;Factura: Fuente;Factura: ID de origen;Factura: ConvergentBillingOperations;Factura: XC_CommodityStatus;Factura: CommodityTransactionDate;Cuenta: XC_CommodityContractID;Factura: EntityInvoiceNumber;Cuenta: Is Commodity Customer";

        protected override string GetTableName() => "IN_SEGUIMIENTO_FACTURAS_B2B";
        protected override string GetTableName2() => "";

        public void ExecuteSeguimientoSemanalB2B(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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
                                                case "InvoiceDate":
                                                    index = 0;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceNumber": 
                                                    index = 1; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceCode": 
                                                    index = 2; 
                                                    value = fields[index]; 
                                                    break;
                                                case "SimplifiedInvoice": 
                                                    index = 3; 
                                                    value = fields[index]; 
                                                    break;
                                                case "ReferenceToOriginalInvoice": 
                                                    index = 4; 
                                                    value = fields[index]; 
                                                    break;
                                                case "RatePlanName": 
                                                    index = 5; 
                                                    value = fields[index]; 
                                                    break;
                                                case "AccountNumber": 
                                                    index = 6; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceEndDate": 
                                                    index = 7; 
                                                    value = fields[index]; 
                                                    break;
                                                case "isGuest": 
                                                    index = 8; 
                                                    value = fields[index]; 
                                                    break;
                                                case "IdentityType": 
                                                    index = 9; 
                                                    value = fields[index]; 
                                                    break;
                                                case "IdentityNumber": 
                                                    index = 10; 
                                                    value = fields[index]; 
                                                    break;
                                                case "EstadoProvincia": 
                                                    index = 11; 
                                                    value = fields[index]; 
                                                    break;
                                                case "AccountName": 
                                                    index = 12; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceAmount": 
                                                    index = 13; 
                                                    value = fields[index]; 
                                                    break;
                                                case "ImportesSinImpuestos": 
                                                    index = 14; 
                                                    value = fields[index]; 
                                                    break;
                                                case "ImporteDelImpuesto": 
                                                    index = 15; 
                                                    value = fields[index]; 
                                                    break;
                                                case "DoxeeTaxRate": 
                                                    index = 16; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceSaldo": 
                                                    index = 17; 
                                                    value = fields[index]; 
                                                    break;
                                                case "Divisa": 
                                                    index = 18; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceStatus": 
                                                    index = 19; 
                                                    value = fields[index]; 
                                                    break;
                                                case"InvoicePURL":
                                                    index = 20; 
                                                    value = fields[index]; 
                                                    break; 
                                                case"idPURL":
                                                    index = 20;
                                                    value = fields[index];
                                                    var idPurl = 0;
                                                    if (string.IsNullOrEmpty(value))
                                                    {
                                                        idPurl = 0;
                                                    }
                                                    else
                                                    {
                                                        idPurl = 1;
                                                    }
                                                    value = idPurl.ToString();
                                                    break;
                                                case "OtherPaymentMethodType": 
                                                    index = 21; 
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
                                                case "TipoTarjetaCredito": 
                                                    index = 22; 
                                                    value = fields[index]; 
                                                    break;
                                                case "NumeroMascaraTarjetaCredito": 
                                                    index = 23; 
                                                    value = fields[index]; 
                                                    break;
                                                case "CorreoElectronicoTrabajo": 
                                                    index = 24; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceAddresseeName": 
                                                    index = 25; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceUpdateDate": 
                                                    index = 26; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceIncluyeUso": 
                                                    index = 27; 
                                                    value = fields[index]; 
                                                    break;
                                                case "AccountCreationDate": 
                                                    index = 28; 
                                                    value = fields[index]; 
                                                    break;
                                                case "LegalEntity": 
                                                    index = 29; 
                                                    value = fields[index]; 
                                                    break;
                                                case "EntityAccountNumber": 
                                                    index = 30; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceCreationDate": 
                                                    index = 31; 
                                                    value = fields[index]; 
                                                    break;
                                                case "MetodoPagoPredeterminado": 
                                                    index = 32; 
                                                    value = fields[index]; 
                                                    break;
                                                case "CopiaPreferenciasEntregaFactura": 
                                                    index = 33; 
                                                    value = fields[index]; 
                                                    break;
                                                case "CorreoParaPreferenciasEntrega": 
                                                    index = 34; 
                                                    value = fields[index]; 
                                                    break;
                                                case "DiaCicloFacturacion":
                                                    index = 35; 
                                                    value = fields[index]; 
                                                    break;
                                                case "DunningCategory": 
                                                    index = 36; 
                                                    value = fields[index]; 
                                                    break;
                                                case "SeverityLevel": 
                                                    index = 37; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceSource": 
                                                    index = 39; 
                                                    value = fields[index]; 
                                                    break;
                                                case "InvoiceID": 
                                                    index = 40; 
                                                    value = fields[index]; 
                                                    break;
                                                case "ConvergentBillingOperations": 
                                                    index = 41;
                                                    value = fields[index];
                                                    //if (value.Length > 5) value = fields[index].Substring(1, 4);
                                                    break;
                                                case "XC_CommodityStatus": 
                                                    index = 42; 
                                                    value = fields[index]; 
                                                    break;
                                                case "CommodityTransactionDate": 
                                                    index = 43; 
                                                    value = fields[index]; 
                                                    break;
                                                case "XC_CommodityContractID": 
                                                    index = 44; 
                                                    value = fields[index]; 
                                                    break;
                                                case "EntityInvoiceNumber": 
                                                    index = 45; 
                                                    value = fields[index]; 
                                                    break;
                                                case "idCommodityCostumer": 
                                                    index = 46;
                                                    value = fields[index];
                                                    var idCommodityCostumer = 0;
                                                    if (value.ToUpper() == "YES") idCommodityCostumer = 1;
                                                    if (value.ToUpper() == "NO") idCommodityCostumer = 2;
                                                    value = idCommodityCostumer.ToString();
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

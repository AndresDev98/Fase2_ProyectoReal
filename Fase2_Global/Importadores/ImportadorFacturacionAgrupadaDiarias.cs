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
    public class ImportadorFacturacionAgrupadaDiarias : Importador
    {
        public ImportadorFacturacionAgrupadaDiarias(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 1;

        protected override string GetFicheroTipo() => "Facturas Agrupadas B2C";

        protected override string GetHeader() => "Account: Account Number;Account: Name;Account: Identity Type;Account: Identity Number;Account: Batch;Account: Delivery Channel;Method of payment;Invoice: Invoice Number;Invoice: Status;Invoice: Invoice Date;Invoice: Code;Invoice: Purl;Invoice: Source;Invoice: Convergent Billing Operations;Invoice: Reference To Original Invoice;Invoice: Amount;Subscription: Name;Subscription: CRM Contract Number;Product: Name;Rate Plan: Name;Rate Plan Charge: Invoice Description;Rate Plan Charge: Billing Period;Invoice Item: Service Start Date;Invoice Item: Service End Date;Contract: State;Rate Plan Charge: Charge Model;Invoice Item: ID;Invoice Item: Applied To Invoice Item Id;WBE: Product Family;Invoice Item: Charge Amount;Invoice Item: Tax Amount;Importe neto descontado;Importe impuesto descontado;Total (descuento e impuesto)";

        protected override string GetTableName() => "IN_FACTURACION_AGRUPADA";
        protected override string GetTableName2() => "IN_FACTURACION_AGRUPADA_PERIODOS";

        public void ExecuteFacturacionAgrupadaDiarias(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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
                            var datatableTemplateAgrupacionPeriodos = database.GetData($"select top 0 * from {GetTableName2()}");
                            var datatableAgrupacionPeriodos = datatableTemplateAgrupacionPeriodos.Clone();

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
                                    //Tabla DRAFTS
                                    for (int i = 0; i < datatable.Columns.Count; i++)
                                    {
                                        try
                                        {
                                            #region Lectura Columnas Fichero
                                            //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                            switch (datatable.Columns[i].ColumnName)
                                            {
                                                case "AccountNumber":
                                                    index = 0;
                                                    value = fields[index];
                                                    break;
                                                case "AccountName":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "IdentityType":
                                                    index = 2;
                                                    value = fields[index];
                                                    break;
                                                case "IdentityNumber":
                                                    index = 3;
                                                    value = fields[index];
                                                    break;
                                                case "AccountBatch":
                                                    index = 4;
                                                    value = fields[index];
                                                    break;
                                                case "DeliveryChannel":
                                                    index = 5;
                                                    value = fields[index];
                                                    break;
                                                case "idDelivery":
                                                    index = 5;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "email":
                                                            value = "1";
                                                            break;
                                                        case "print":
                                                            value = "2";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "MethodOfPayment":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 7;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceStatus":
                                                    index = 8;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDate":
                                                    index = 9;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceCode":
                                                    index = 10;
                                                    value = fields[index];
                                                    break;
                                                case "InvoicePURL":
                                                    index = 11;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceSource":
                                                    index = 12;
                                                    value = fields[index];
                                                    break;
                                                case "ConvergentBillingOperations":
                                                    index = 13;
                                                    value = fields[index];
                                                    //if (value.Length > 5) value = fields[index].Substring(1, 4);
                                                    break;
                                                case "ReferenceToOriginalInvoice":
                                                    index = 14;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAmount":
                                                    index = 15;
                                                    value = fields[index];
                                                    break;
                                                case "SubscriptionName":
                                                    index = 16;
                                                    value = fields[index];
                                                    break;
                                                case "CRMContractNumber":
                                                    index = 17;
                                                    value = fields[index];
                                                    break;
                                                case "ProductName":
                                                    index = 18;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 19;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDescription":
                                                    index = 20;
                                                    value = fields[index];
                                                    break;
                                                case "BillingPeriod":
                                                    index = 21;
                                                    value = fields[index];
                                                    break;
                                                case "idBillingPeriod":
                                                    index = 21;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "Month":
                                                            value = "1";
                                                            break;
                                                        case "Specific Months":
                                                            value = "2";
                                                            break;
                                                        case "Annual":
                                                            value = "3";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "ServiceStartDate":
                                                    index = 22;
                                                    value = fields[index];
                                                    break;
                                                case "ServiceEndDate":
                                                    index = 23;
                                                    value = fields[index];
                                                    break;
                                                case "ContractState":
                                                    index = 24;
                                                    value = fields[index];
                                                    break;
                                                case "ChargeModel":
                                                    index = 25;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceID":
                                                    index = 26;
                                                    value = fields[index];
                                                    break;
                                                case "AppliedToInvoiceItemId":
                                                    index = 27;
                                                    value = fields[index];
                                                    break;
                                                case "ProductFamily":
                                                    index = 28;
                                                    value = fields[index];
                                                    break;
                                                case "ChargeAmount":
                                                    index = 29;
                                                    value = fields[index];
                                                    break;
                                                case "TaxAmount":
                                                    index = 30;
                                                    value = fields[index];
                                                    break;
                                                case "ImporteNetoDescontado":
                                                    index = 31;
                                                    value = fields[index];
                                                    break;
                                                case "ImporteImpuestoDescontado":
                                                    index = 32;
                                                    value = fields[index];
                                                    break;
                                                case "Total":
                                                    index = 33;
                                                    value = fields[index];
                                                    break;
                                                case "filedate":
                                                    value = fechaFichero.ToString();//Constante
                                                    break;
                                                case "idFamily":
                                                    value = "0";//Constante
                                                    break;
                                                case "idCommodityCostumer":
                                                    value = "999";//Constante
                                                    break;
                                                case "idSegmento":
                                                    value = "2";//Constante
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

                                    //---------------------------------------------------
                                    //Tabla AGRUPACION PERIODOS
                                    var arrayAgrupacionPeriodos = new object[datatableAgrupacionPeriodos.Columns.Count];
                                    var valueAgrupacionPeriodos = "";
                                    DateTime InvoiceDate = new DateTime();
                                    DateTime ServiceStartDate = new DateTime();
                                    DateTime ServiceEndDate = new DateTime();
                                    //Calculos de dias/meses
                                    //DateTime.TryParseExact(fields[9], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime InvoiceDate);
                                    if (DateTime.TryParseExact(fields[9], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime InvoiceDate1) == true)
                                        InvoiceDate = InvoiceDate1;
                                    else
                                    {
                                        if (DateTime.TryParseExact(fields[9], "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime InvoiceDate2) == true)
                                            InvoiceDate = InvoiceDate2;
                                    }
                                    //DateTime.TryParseExact(fields[22], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceStartDate);
                                    if (DateTime.TryParseExact(fields[22], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceStartDate1) == true)
                                        ServiceStartDate = ServiceStartDate1;
                                    else
                                    {
                                        if (DateTime.TryParseExact(fields[22], "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceStartDate2) == true)
                                            ServiceStartDate = ServiceStartDate2;
                                    }
                                    //DateTime.TryParseExact(fields[23], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceEndDate);
                                    if (DateTime.TryParseExact(fields[23], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceEndDate1) == true)
                                        ServiceEndDate = ServiceEndDate1;
                                    else
                                    {
                                        if (DateTime.TryParseExact(fields[23], "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime ServiceEndDate2) == true)
                                            ServiceEndDate = ServiceEndDate2;
                                    }

                                    var DiferenciaDiasProrrateo = 0;
                                    var DiferenciaDiasPlazo = 0;
                                    var DiferenciaMesesProrrateo = 0;
                                    var DiferenciaMesesPlazo = 0;
                                    var DiferenciaMesesInvoiceDateServiceEndDate = 0;
                                    var DiferenciaMesesInvoiceDateServiceStartDate = 0;

                                    DiferenciaDiasProrrateo = (ServiceEndDate - ServiceStartDate).Days;
                                    DiferenciaDiasPlazo = (InvoiceDate - ServiceStartDate).Days;
                                    DiferenciaMesesProrrateo = (ServiceEndDate.Month - ServiceStartDate.Month) + 12 * (ServiceEndDate.Year - ServiceStartDate.Year);
                                    DiferenciaMesesPlazo = (InvoiceDate.Month - ServiceStartDate.Month) + 12 * (InvoiceDate.Year - ServiceStartDate.Year);
                                    DiferenciaMesesInvoiceDateServiceStartDate = (InvoiceDate.Month - ServiceStartDate.Month) + 12 * (InvoiceDate.Year - ServiceStartDate.Year);
                                    DiferenciaMesesInvoiceDateServiceEndDate = (InvoiceDate.Month - ServiceEndDate.Month) + 12 * (InvoiceDate.Year - ServiceEndDate.Year);

                                    //Si el Total (descuento e impuesto) es 0. No se hará nada
                                    if (fields[33] != "0")
                                    {
                                        //Si diferenciaMesesProrrateo es <=1. PRORRATEADO
                                        if (DiferenciaMesesProrrateo <= 1)
                                        {
                                            #region DiferenciaMesesProrrateo <=1
                                            for (int i = 0; i < datatable.Columns.Count; i++)
                                            {
                                                try
                                                {
                                                    #region Lectura Columnas Fichero
                                                    //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                                    switch (datatableAgrupacionPeriodos.Columns[i].ColumnName)
                                                    {
                                                        case "AccountNumber":
                                                            index = 0;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "AccountName":
                                                            index = 1;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "IdentityType":
                                                            index = 2;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "IdentityNumber":
                                                            index = 3;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "AccountBatch":
                                                            index = 4;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "DeliveryChannel":
                                                            index = 5;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "idDelivery":
                                                            index = 5;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            switch (value)
                                                            {
                                                                case "email":
                                                                    valueAgrupacionPeriodos = "1";
                                                                    break;
                                                                case "print":
                                                                    valueAgrupacionPeriodos = "2";
                                                                    break;
                                                                default:
                                                                    valueAgrupacionPeriodos = "0";
                                                                    break;
                                                            }
                                                            break;
                                                        case "MethodOfPayment":
                                                            index = 6;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceNumber":
                                                            index = 7;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceStatus":
                                                            index = 8;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceDate":
                                                            index = 9;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceCode":
                                                            index = 10;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoicePURL":
                                                            index = 11;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceSource":
                                                            index = 12;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ConvergentBillingOperations":
                                                            index = 13;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            if (valueAgrupacionPeriodos.Length > 5) valueAgrupacionPeriodos = fields[index].Substring(1, 4);
                                                            break;
                                                        case "ReferenceToOriginalInvoice":
                                                            index = 14;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceAmount":
                                                            index = 15;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "SubscriptionName":
                                                            index = 16;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "CRMContractNumber":
                                                            index = 17;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ProductName":
                                                            index = 18;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "RatePlanName":
                                                            index = 19;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceDescription":
                                                            index = 20;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "BillingPeriod":
                                                            index = 21;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "idBillingPeriod":
                                                            index = 21;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            switch (valueAgrupacionPeriodos)
                                                            {
                                                                case "Month":
                                                                    valueAgrupacionPeriodos = "1";
                                                                    break;
                                                                case "Specific Months":
                                                                    valueAgrupacionPeriodos = "2";
                                                                    break;
                                                                case "Annual":
                                                                    valueAgrupacionPeriodos = "3";
                                                                    break;
                                                                default:
                                                                    valueAgrupacionPeriodos = "0";
                                                                    break;
                                                            }
                                                            break;
                                                        case "ServiceStartDate":
                                                            index = 22;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ServiceEndDate":
                                                            index = 23;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ContractState":
                                                            index = 24;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ChargeModel":
                                                            index = 25;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "InvoiceID":
                                                            index = 26;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "AppliedToInvoiceItemId":
                                                            index = 27;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ProductFamily":
                                                            index = 28;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ChargeAmount":
                                                            index = 29;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "TaxAmount":
                                                            index = 30;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ImporteNetoDescontado":
                                                            index = 31;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "ImporteImpuestoDescontado":
                                                            index = 32;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "Total":
                                                            index = 33;
                                                            valueAgrupacionPeriodos = fields[index];
                                                            break;
                                                        case "filedate":
                                                            valueAgrupacionPeriodos = fechaFichero.ToString();//Constante
                                                            break;
                                                        case "idFamily":
                                                            valueAgrupacionPeriodos = "0";//Constante
                                                            break;
                                                        case "idCommodityCostumer":
                                                            valueAgrupacionPeriodos = "999";//Constante
                                                            break;
                                                        case "idSegmento":
                                                            valueAgrupacionPeriodos = "2";//Constante
                                                            break;
                                                        case "Fecha Factura Prorrateada DIM":
                                                            valueAgrupacionPeriodos = new DateTime(ServiceStartDate.Year, ServiceStartDate.Month, 1).ToString();
                                                            break;
                                                        case "MesAnioProrrateado":
                                                            valueAgrupacionPeriodos = $"{CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(ServiceStartDate.Month)}-{ServiceStartDate.Year}";
                                                            break;
                                                        case "idMesAnio":
                                                            valueAgrupacionPeriodos = ServiceStartDate.Month.ToString() + ServiceStartDate.Year.ToString();
                                                            break;
                                                        case "MesesDiferencia":
                                                            valueAgrupacionPeriodos = DiferenciaMesesProrrateo.ToString();
                                                            break;
                                                        case "Diferencia Meses InvoiceDate-ServiceStartDate":
                                                            valueAgrupacionPeriodos = DiferenciaMesesInvoiceDateServiceStartDate.ToString();
                                                            break;
                                                        case "DiferenciaDiasPlazo":
                                                            valueAgrupacionPeriodos = DiferenciaDiasPlazo.ToString();
                                                            break;
                                                        case "idDentroPlazo":
                                                            //Obtenemos idBillingPeriod
                                                            switch (fields[21])
                                                            {
                                                                case "Month": //idBillingPeriod=1
                                                                    if (InvoiceDate.Year == ServiceEndDate.Year)
                                                                    {
                                                                        if (DiferenciaMesesInvoiceDateServiceEndDate <= 1)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        if (DiferenciaMesesInvoiceDateServiceEndDate == -11)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                    }
                                                                    break;
                                                                case "Specific Months": //idBillingPeriod=2
                                                                    if (InvoiceDate.Year == ServiceEndDate.Year)
                                                                    {
                                                                        if (DiferenciaMesesInvoiceDateServiceEndDate <= 2)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        if (DiferenciaMesesInvoiceDateServiceEndDate == -11 || DiferenciaMesesInvoiceDateServiceEndDate == -10)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                    }
                                                                    break;
                                                                case "Annual": //idBillingPeriod=3
                                                                    if (ServiceStartDate <= InvoiceDate)
                                                                    {
                                                                        valueAgrupacionPeriodos = "1";
                                                                    }
                                                                    else
                                                                    {
                                                                        valueAgrupacionPeriodos = "0";
                                                                    }
                                                                    break;
                                                                default: //idBillingPeriod=0
                                                                    if (DiferenciaDiasPlazo <= DiferenciaDiasProrrateo)
                                                                    {
                                                                        valueAgrupacionPeriodos = "1";
                                                                    }
                                                                    else
                                                                    {
                                                                        valueAgrupacionPeriodos = "0";
                                                                    }
                                                                    break;
                                                            }
                                                            break;
                                                        //------ Solo hay 1 copia (NO PRORRATEADO)
                                                        case "InvoiceAmountProrrateado":
                                                            valueAgrupacionPeriodos = fields[33]; //Campo "Total (descuento e impuesto)"
                                                            break;
                                                        case "NumCopia":
                                                            valueAgrupacionPeriodos = "0";
                                                            break;
                                                        case "FechaFacturaProrrateada":
                                                            valueAgrupacionPeriodos = ServiceStartDate.ToString();
                                                            break;
                                                        case "Anio":
                                                            valueAgrupacionPeriodos = ServiceStartDate.Year.ToString();
                                                            break;
                                                        case "Mes":
                                                            valueAgrupacionPeriodos = ServiceStartDate.Month.ToString();
                                                            break;
                                                        //------ Solo hay 1 copia (NO PRORRATEADO)

                                                        default:
                                                            index = 999;
                                                            valueAgrupacionPeriodos = "";
                                                            break;
                                                    }
                                                    #endregion
                                                    #region Conversión Dato
                                                    if (valueAgrupacionPeriodos == "")
                                                        arrayAgrupacionPeriodos[i] = DBNull.Value;
                                                    else
                                                    {

                                                        switch (datatableAgrupacionPeriodos.Columns[i].DataType.ToString())
                                                        {
                                                            case "System.DateTime":
                                                                {
                                                                    arrayAgrupacionPeriodos[i] = ConvierteDatoFecha(valueAgrupacionPeriodos);                                                                    
                                                                }
                                                                break;

                                                            case "System.String":
                                                                arrayAgrupacionPeriodos[i] = valueAgrupacionPeriodos;
                                                                break;

                                                            case "System.Decimal":
                                                                {
                                                                    if (valueAgrupacionPeriodos.StartsWith("-"))
                                                                        valueAgrupacionPeriodos = "-" + valueAgrupacionPeriodos.Substring(1, valueAgrupacionPeriodos.Length - 1).Trim();

                                                                    valueAgrupacionPeriodos = valueAgrupacionPeriodos.Replace(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                                                                    valueAgrupacionPeriodos = valueAgrupacionPeriodos.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

                                                                    if (valueAgrupacionPeriodos.Split(',').Count() > 2)
                                                                    {
                                                                        var regex = new Regex(Regex.Escape(","));
                                                                        valueAgrupacionPeriodos = regex.Replace(valueAgrupacionPeriodos, "", 1);
                                                                    }

                                                                    arrayAgrupacionPeriodos[i] = double.Parse(valueAgrupacionPeriodos, NumberStyles.Float, CultureInfo.CurrentCulture);
                                                                }
                                                                break;

                                                            case "System.Int32":
                                                                arrayAgrupacionPeriodos[i] = int.Parse(valueAgrupacionPeriodos);
                                                                break;

                                                            default:
                                                                throw new Exception($"Tipo {datatableAgrupacionPeriodos.Columns[i].DataType.ToString()} no soportado en la lectura");
                                                        }
                                                    }
                                                    #endregion
                                                }
                                                catch (Exception e)
                                                {
                                                    throw new Exception($"Error durante la lectura y proceso de datos del fichero '{fileName}' en la linea {row} para el campo(BBDD): {datatableAgrupacionPeriodos.Columns[i].ColumnName}. ERROR: {e.Message}");
                                                }
                                            }
                                            #endregion
                                        }
                                        else
                                        {
                                            #region DiferenciaMesesProrrateo > 1
                                            //Generar Copias/Clones de la linea

                                            for (int numCopia = 1; numCopia <= DiferenciaMesesProrrateo; numCopia++)
                                            {
                                                for (int i = 0; i < datatable.Columns.Count; i++)
                                                {
                                                    try
                                                    {
                                                        #region Lectura Columnas Fichero
                                                        //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                                        switch (datatableAgrupacionPeriodos.Columns[i].ColumnName)
                                                        {
                                                            case "AccountNumber":
                                                                index = 0;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "AccountName":
                                                                index = 1;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "IdentityType":
                                                                index = 2;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "IdentityNumber":
                                                                index = 3;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "AccountBatch":
                                                                index = 4;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "DeliveryChannel":
                                                                index = 5;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "idDelivery":
                                                                index = 5;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                switch (value)
                                                                {
                                                                    case "email":
                                                                        valueAgrupacionPeriodos = "1";
                                                                        break;
                                                                    case "print":
                                                                        valueAgrupacionPeriodos = "2";
                                                                        break;
                                                                    default:
                                                                        valueAgrupacionPeriodos = "0";
                                                                        break;
                                                                }
                                                                break;
                                                            case "MethodOfPayment":
                                                                index = 6;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceNumber":
                                                                index = 7;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceStatus":
                                                                index = 8;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceDate":
                                                                index = 9;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceCode":
                                                                index = 10;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoicePURL":
                                                                index = 11;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceSource":
                                                                index = 12;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ConvergentBillingOperations":
                                                                index = 13;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                if (valueAgrupacionPeriodos.Length > 5) valueAgrupacionPeriodos = fields[index].Substring(1, 4);
                                                                break;
                                                            case "ReferenceToOriginalInvoice":
                                                                index = 14;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceAmount":
                                                                index = 15;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "SubscriptionName":
                                                                index = 16;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "CRMContractNumber":
                                                                index = 17;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ProductName":
                                                                index = 18;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "RatePlanName":
                                                                index = 19;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceDescription":
                                                                index = 20;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "BillingPeriod":
                                                                index = 21;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "idBillingPeriod":
                                                                index = 21;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                switch (valueAgrupacionPeriodos)
                                                                {
                                                                    case "Month":
                                                                        valueAgrupacionPeriodos = "1";
                                                                        break;
                                                                    case "Specific Months":
                                                                        valueAgrupacionPeriodos = "2";
                                                                        break;
                                                                    case "Annual":
                                                                        valueAgrupacionPeriodos = "3";
                                                                        break;
                                                                    default:
                                                                        valueAgrupacionPeriodos = "0";
                                                                        break;
                                                                }
                                                                break;
                                                            case "ServiceStartDate":
                                                                index = 22;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ServiceEndDate":
                                                                index = 23;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ContractState":
                                                                index = 24;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ChargeModel":
                                                                index = 25;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "InvoiceID":
                                                                index = 26;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "AppliedToInvoiceItemId":
                                                                index = 27;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ProductFamily":
                                                                index = 28;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ChargeAmount":
                                                                index = 29;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "TaxAmount":
                                                                index = 30;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ImporteNetoDescontado":
                                                                index = 31;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "ImporteImpuestoDescontado":
                                                                index = 32;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "Total":
                                                                index = 33;
                                                                valueAgrupacionPeriodos = fields[index];
                                                                break;
                                                            case "filedate":
                                                                valueAgrupacionPeriodos = fechaFichero.ToString();//Constante
                                                                break;
                                                            case "idFamily":
                                                                valueAgrupacionPeriodos = "0";//Constante
                                                                break;
                                                            case "idCommodityCostumer":
                                                                valueAgrupacionPeriodos = "999";//Constante
                                                                break;
                                                            case "idSegmento":
                                                                valueAgrupacionPeriodos = "2";//Constante
                                                                break;
                                                            case "Fecha Factura Prorrateada DIM":
                                                                valueAgrupacionPeriodos = new DateTime(ServiceStartDate.Year, ServiceStartDate.Month, 1).ToString();
                                                                break;
                                                            case "MesAnioProrrateado":
                                                                valueAgrupacionPeriodos = $"{CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(ServiceStartDate.Month)}-{ServiceStartDate.Year}";
                                                                break;
                                                            case "idMesAnio":
                                                                valueAgrupacionPeriodos = ServiceStartDate.Month.ToString() + ServiceStartDate.Year.ToString();
                                                                break;
                                                            case "MesesDiferencia":
                                                                valueAgrupacionPeriodos = DiferenciaMesesProrrateo.ToString();
                                                                break;
                                                            case "Diferencia Meses InvoiceDate-ServiceStartDate":
                                                                valueAgrupacionPeriodos = DiferenciaMesesInvoiceDateServiceStartDate.ToString();
                                                                break;
                                                            case "DiferenciaDiasPlazo":
                                                                valueAgrupacionPeriodos = DiferenciaDiasPlazo.ToString();
                                                                break;
                                                            case "idDentroPlazo":
                                                                //Obtenemos idBillingPeriod
                                                                switch (fields[21])
                                                                {
                                                                    case "Month": //idBillingPeriod=1
                                                                        if (InvoiceDate.Year == ServiceEndDate.Year)
                                                                        {
                                                                            if (DiferenciaMesesInvoiceDateServiceEndDate <= 1)
                                                                            {
                                                                                valueAgrupacionPeriodos = "1";
                                                                            }
                                                                            else
                                                                            {
                                                                                valueAgrupacionPeriodos = "0";
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            if (DiferenciaMesesInvoiceDateServiceEndDate == -11)
                                                                            {
                                                                                valueAgrupacionPeriodos = "1";
                                                                            }
                                                                            else
                                                                            {
                                                                                valueAgrupacionPeriodos = "0";
                                                                            }
                                                                        }
                                                                        break;
                                                                    case "Specific Months": //idBillingPeriod=2
                                                                        if (InvoiceDate.Year == ServiceEndDate.Year)
                                                                        {
                                                                            if (DiferenciaMesesInvoiceDateServiceEndDate <= 2)
                                                                            {
                                                                                valueAgrupacionPeriodos = "1";
                                                                            }
                                                                            else
                                                                            {
                                                                                valueAgrupacionPeriodos = "0";
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            if (DiferenciaMesesInvoiceDateServiceEndDate == -11 || DiferenciaMesesInvoiceDateServiceEndDate == -10)
                                                                            {
                                                                                valueAgrupacionPeriodos = "1";
                                                                            }
                                                                            else
                                                                            {
                                                                                valueAgrupacionPeriodos = "0";
                                                                            }
                                                                        }
                                                                        break;
                                                                    case "Annual": //idBillingPeriod=3
                                                                        if (ServiceStartDate <= InvoiceDate)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                        break;
                                                                    default: //idBillingPeriod=0
                                                                        if (DiferenciaDiasPlazo <= DiferenciaDiasProrrateo)
                                                                        {
                                                                            valueAgrupacionPeriodos = "1";
                                                                        }
                                                                        else
                                                                        {
                                                                            valueAgrupacionPeriodos = "0";
                                                                        }
                                                                        break;
                                                                }
                                                                break;
                                                            //------ Diferentes en cada copia (PRORRATEADO)
                                                            case "InvoiceAmountProrrateado":
                                                                valueAgrupacionPeriodos = decimal.Round(Convert.ToDecimal((Convert.ToDouble(fields[33]) / DiferenciaMesesProrrateo)),2).ToString(); //Campo "Total (descuento e impuesto)"
                                                                break;
                                                            case "NumCopia":
                                                                valueAgrupacionPeriodos = numCopia.ToString();
                                                                break;
                                                            case "FechaFacturaProrrateada":
                                                                valueAgrupacionPeriodos = ServiceStartDate.AddMonths(numCopia).ToString();
                                                                break;
                                                            case "Anio":
                                                                valueAgrupacionPeriodos = ServiceStartDate.AddMonths(numCopia).Year.ToString();
                                                                break;
                                                            case "Mes":
                                                                valueAgrupacionPeriodos = ServiceStartDate.AddMonths(numCopia).Month.ToString();
                                                                break;
                                                            //------ Diferentes en cada copia (PRORRATEADO)

                                                            default:
                                                                index = 999;
                                                                valueAgrupacionPeriodos = "";
                                                                break;
                                                        }
                                                        #endregion
                                                        #region Conversión Dato
                                                        if (valueAgrupacionPeriodos == "")
                                                            arrayAgrupacionPeriodos[i] = DBNull.Value;
                                                        else
                                                        {

                                                            switch (datatableAgrupacionPeriodos.Columns[i].DataType.ToString())
                                                            {
                                                                case "System.DateTime":
                                                                    {
                                                                        arrayAgrupacionPeriodos[i] = ConvierteDatoFecha(valueAgrupacionPeriodos);
                                                                    }
                                                                    break;

                                                                case "System.String":
                                                                    arrayAgrupacionPeriodos[i] = valueAgrupacionPeriodos;
                                                                    break;

                                                                case "System.Decimal":
                                                                    {
                                                                        if (valueAgrupacionPeriodos.StartsWith("-"))
                                                                            valueAgrupacionPeriodos = "-" + valueAgrupacionPeriodos.Substring(1, valueAgrupacionPeriodos.Length - 1).Trim();

                                                                        valueAgrupacionPeriodos = valueAgrupacionPeriodos.Replace(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                                                                        valueAgrupacionPeriodos = valueAgrupacionPeriodos.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

                                                                        if (valueAgrupacionPeriodos.Split(',').Count() > 2)
                                                                        {
                                                                            var regex = new Regex(Regex.Escape(","));
                                                                            valueAgrupacionPeriodos = regex.Replace(valueAgrupacionPeriodos, "", 1);
                                                                        }

                                                                        arrayAgrupacionPeriodos[i] = double.Parse(valueAgrupacionPeriodos, NumberStyles.Float, CultureInfo.CurrentCulture);
                                                                    }
                                                                    break;

                                                                case "System.Int32":
                                                                    arrayAgrupacionPeriodos[i] = int.Parse(valueAgrupacionPeriodos);
                                                                    break;

                                                                default:
                                                                    throw new Exception($"Tipo {datatableAgrupacionPeriodos.Columns[i].DataType.ToString()} no soportado en la lectura");
                                                            }
                                                        }
                                                        #endregion
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        throw new Exception($"Error durante la lectura y proceso de datos del fichero '{fileName}' en la linea {row} para el campo(BBDD): {datatableAgrupacionPeriodos.Columns[i].ColumnName}. ERROR: {e.Message}");
                                                    }
                                                }
                                            }
                                            #endregion
                                        }


                                        datatableAgrupacionPeriodos.Rows.Add(arrayAgrupacionPeriodos);
                                        if (datatableAgrupacionPeriodos.Rows.Count > bufferSize)
                                        {
                                            database.Bulk(datatableAgrupacionPeriodos, GetTableName2());
                                            datatableAgrupacionPeriodos = datatableTemplateAgrupacionPeriodos.Clone();
                                        }
                                    }
                                }
                                line = sr.ReadLine();
                            }

                            if (datatable.Rows.Count > 0)
                                database.Bulk(datatable, GetTableName());
                            if (datatableAgrupacionPeriodos.Rows.Count > 0)
                                database.Bulk(datatableAgrupacionPeriodos, GetTableName2());

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

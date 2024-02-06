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
    public class ImportadorDraftsDiarios : Importador
    {
        public ImportadorDraftsDiarios(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 8;

        protected override string GetFicheroTipo() => "Draft";

        protected override string GetHeader() => "Invoice: Invoice Date;Invoice Item: Service Start Date;Invoice Item: Service End Date;Account: XC_CommodityFrequency;Product Rate Plan Charge: Charge Type;Product Rate Plan Charge: Billing Period;Invoice: Invoice Number;Subscription: Name;Invoice: Code;Account: Simplified Invoice;Invoice: Reference To Original Invoice;Invoice: Rate Plan Name;Account: Account Number;Invoice: Due Date;Account: is Guest;Account: Identity Type;Subscription: Everest_Contract_Id;Account: Identity Number;Bill To: State/Province;Account: Name;Invoice: Amount;Invoice: Amount Without Tax;Invoice: Tax Amount;Invoice: Doxee Tax Rate;Invoice: Balance;Account: Currency;Invoice: Status;Invoice: PURL;Account: otherPaymentMethodType;Default Payment Method: Credit Card Type;Default Payment Method: Credit Card Mask Number;Sold To: Work Email;Bill To: First Name;Invoice: Updated Date;Invoice: Includes Usage;Account: Created Date;Account: Legal Entity;Account: EntityAccountNumber;Invoice: Validationerrorcode;Invoice: Created Date;Default Payment Method: Type;Account: Invoice Delivery Preferences Print;Account: Invoice Delivery Preferences Email;Invoice: Validationerrordescription;Journal Entry: Subscription Rate Plan Charge WBE;Invoice: EntityInvoiceNumber;Invoice: Source;Invoice: Source ID;Account: Is Commodity Customer;Rate Plan Charge: Invoice Description;Invoice: ID;Account: ID";

        protected override string GetTableName() => "IN_DRAFTS";
        protected override string GetTableName2() => "IN_DRAFTS_VALIDATIONERROR";

        public void ExecuteDraftsDiarios(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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
                            var datatableTemplateValidationError = database.GetData($"select top 0 * from {GetTableName2()}");
                            var datatableValidationError = datatableTemplateValidationError.Clone();

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
                                    var ratePlanName = "";
                                    var invoiceDescription = "";
                                    var familyREGEX = "";
                                    bool contienePalabra = false;
                                    bool noContienePalabra = false;
                                    //Tabla DRAFTS
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
                                                case "idBillingPeriod":
                                                    index = 5;
                                                    value = fields[index];
                                                    //Obtención idBillingPeriod
                                                    var idChargeType = 0;
                                                    var idBillingPeriod = 4;
                                                    switch (value)
                                                    {
                                                        case "Month":
                                                            idChargeType = 1;
                                                            break;
                                                        case "Specific Months":
                                                            idChargeType = 2;
                                                            break;
                                                        case "Annual":
                                                            idChargeType = 3;
                                                            break;
                                                    }

                                                    ratePlanName = fields[11];
                                                    invoiceDescription = fields[49];
                                                    familyREGEX = ratePlanName;
                                                    if (ratePlanName.Equals("disco") || string.IsNullOrEmpty(ratePlanName) || ratePlanName.Equals("Discount*"))
                                                        familyREGEX = invoiceDescription;

                                                    contienePalabra = "a tu ritmo/ a tu ritmo promo/saba/bamsa/a tu ritmo smart/a tu ritmo pro/rfid"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                    if (idChargeType == 1 && contienePalabra) idBillingPeriod = 5;

                                                    contienePalabra = "juicepass/a tu ritmo plus/a tur itmo plus/vinculado saba/vinculado bamsa/a tu ritmo smart/a tu ritmo pro/rfid/a tu ritmo flotas"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                    if (idChargeType == 1 && contienePalabra) idBillingPeriod = 1;
                                                    if (idChargeType == 2) idBillingPeriod = 5;
                                                    if (idChargeType != 1 && idChargeType != 2)
                                                    {
                                                        //Calcular Servince End Date - Service Start Date (en dias)
                                                        if (fields[2] != "" && fields[1] != "")
                                                        {
                                                            DateTime endDate = DateTime.ParseExact(fields[2], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                                            DateTime startDate = DateTime.ParseExact(fields[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                                            int diferenciaDias = (endDate - startDate).Days;
                                                            if (diferenciaDias <= 31)
                                                            {
                                                                idBillingPeriod = 1;
                                                            }
                                                            else
                                                            {
                                                                if (diferenciaDias <= 62)
                                                                {
                                                                    idBillingPeriod = 2;
                                                                }
                                                                else
                                                                {
                                                                    idBillingPeriod = 3;
                                                                }
                                                            }
                                                        }
                                                    }

                                                    value = idBillingPeriod.ToString();
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;
                                                case "SubscriptionName":
                                                    index = 7;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceCode":
                                                    index = 8;
                                                    value = fields[index];
                                                    break;
                                                case "idInvoiceCode":
                                                    index = 8;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "F1":
                                                            value = "1";
                                                            break;
                                                        case "F2":
                                                            value = "2";
                                                            break;
                                                        case "R1":
                                                            value = "3";
                                                            break;
                                                        case "R4":
                                                            value = "4";
                                                            break;
                                                        case "R5":
                                                            value = "5";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "SimplifiedInvoice":
                                                    index = 9;
                                                    value = fields[index];
                                                    break;
                                                case "ReferenceToOriginalInvoice":
                                                    index = 10;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 11;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanNamePARTHNER":
                                                    index = 11;
                                                    value = fields[index];
                                                    if (value.Length > 10) value = fields[index].Substring(0, 9);
                                                    break;
                                                case "AccountNumber":
                                                    index = 12;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDueDate":
                                                    index = 13;
                                                    value = fields[index];
                                                    break;
                                                case "IsGuest":
                                                    index = 14;
                                                    value = fields[index];
                                                    break;
                                                case "idIsGuest":
                                                    index = 14;
                                                    value = fields[index];
                                                    var idIsGuest = 0;
                                                    if (value.ToUpper() == "YES") idIsGuest = 1;
                                                    if (value.ToUpper() == "NO") idIsGuest = 2;
                                                    value = idIsGuest.ToString();
                                                    break;
                                                case "IdentityType":
                                                    index = 15;
                                                    value = fields[index];
                                                    break;
                                                case "Everest_Contract_Id":
                                                    index = 16;
                                                    value = fields[index];
                                                    if (value.Length > 200) value = fields[index].Substring(0, 199);
                                                    value = fields[index];
                                                    break;
                                                case "IdentityNumber":
                                                    index = 17;
                                                    value = fields[index];
                                                    break;
                                                case "StateProvince":
                                                    index = 18;
                                                    value = fields[index];
                                                    break;
                                                case "AccountName":
                                                    index = 19;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAmount":                                                    
                                                    index = 20;
                                                    value = fields[index];
                                                    break;
                                                case "AmountWithoutTax":
                                                    index = 21;
                                                    value = fields[index];
                                                    break;
                                                case "TaxAmount":
                                                    index = 22;
                                                    value = fields[index];
                                                    break;
                                                case "DoxeeTaxRate":
                                                    index = 23;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceBalance":
                                                    index = 24;
                                                    value = fields[index];
                                                    break;
                                                case "Currency":
                                                    index = 25;
                                                    value = fields[index];
                                                    break;
                                                case "InvoicePURL":
                                                    index = 27;
                                                    value = fields[index];
                                                    break;
                                                case "idOtherPaymentMethodType":
                                                    index = 28;
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
                                                case "CreditCardType":
                                                    index = 29;
                                                    value = fields[index];
                                                    break;
                                                case "idCreditCardType":
                                                    index = 29;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "Visa":
                                                            value = "1";
                                                            break;
                                                        case "MasterCard":
                                                            value = "2";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "CreditCardMaskNumber":
                                                    index = 30;
                                                    value = fields[index];
                                                    break;
                                                case "WorkEmail":
                                                    index = 31;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceAddreseName":
                                                    index = 32;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceUpdateDate":
                                                    index = 33;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceIncludesUsage":
                                                    index = 34;
                                                    value = fields[index];
                                                    break;
                                                case "AccountCreationDate":
                                                    index = 35;
                                                    value = fields[index];
                                                    break;
                                                case "AccountLegalEntity":
                                                    index = 36;
                                                    value = fields[index];
                                                    break;
                                                case "idLegalEntity":
                                                    index = 36;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "Endesa X Servicios SL":
                                                            value = "1";
                                                            break;
                                                        case "Endesa Energia SAU":
                                                            value = "2";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "EntityAccountNumber":
                                                    index = 37;
                                                    value = fields[index];
                                                    break;
                                                case "Validationerrorcode":
                                                    index = 38;
                                                    value = fields[index];
                                                    break;
                                                case "idValidationError":
                                                    index = 38;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "A001":
                                                            value = "1";
                                                            break;
                                                        case "A002":
                                                            value = "2";
                                                            break;
                                                        case "A003":
                                                            value = "3";
                                                            break;
                                                        case "A004":
                                                            value = "4";
                                                            break;
                                                        case "A005":
                                                            value = "5";
                                                            break;
                                                        case "A006":
                                                            value = "6";
                                                            break;
                                                        case "A007":
                                                            value = "7";
                                                            break;
                                                        case "A008":
                                                            value = "8";
                                                            break;
                                                        case "A009":
                                                            value = "9";
                                                            break;
                                                        case "A010":
                                                            value = "10";
                                                            break;
                                                        case "A011":
                                                            value = "11";
                                                            break;
                                                        case "A012":
                                                            value = "12";
                                                            break;
                                                        case "A013":
                                                            value = "13";
                                                            break;
                                                        case "A014":
                                                            value = "14";
                                                            break;
                                                        case "A015":
                                                            value = "15";
                                                            break;
                                                        case "M001":
                                                            value = "101";
                                                            break;
                                                        case "M002":
                                                            value = "102";
                                                            break;
                                                        case "M003":
                                                            value = "103";
                                                            break;
                                                        case "P003":
                                                            value = "1003";
                                                            break;
                                                        case "VAL-SII-0002":
                                                            value = "1002";
                                                            break;
                                                        case "VAL-SII-0005":
                                                            value = "1005";
                                                            break;
                                                        case "VAL-SII-0006":
                                                            value = "1006";
                                                            break;
                                                        default:
                                                            value = "9999";
                                                            break;
                                                    }
                                                    break;
                                                case "InvoiceCreationDate":
                                                    index = 39;
                                                    value = fields[index];
                                                    break;
                                                case "DefaultPaymentMethodType":
                                                    index = 40;
                                                    value = fields[index];
                                                    break;
                                                case "idDefaultPaymentMethodType":
                                                    index = 40;
                                                    value = fields[index];
                                                    switch (value)
                                                    {
                                                        case "BankTransfer":
                                                            value = "1";
                                                            break;
                                                        case "CreditCardReferenceTransaction":
                                                            value = "2";
                                                            break;
                                                        case "Other":
                                                            value = "3";
                                                            break;
                                                        default:
                                                            value = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "InvoiceDeliveryPreferencesPrint":
                                                    index = 41;
                                                    value = fields[index];
                                                    if (value.Length > 10) value = fields[index].Substring(0, 9);
                                                    break;
                                                case "InvoiceDeliveryPreferencesEmail":
                                                    index = 42;
                                                    value = fields[index];
                                                    if (value.Length > 10) value = fields[index].Substring(0, 9);
                                                    break;
                                                case "ValidationErrorDescription":
                                                    index = 43;
                                                    value = fields[index];
                                                    break;
                                                case "idValidacion":
                                                    index = 43;
                                                    value = fields[index];
                                                    //Si contiene Manual -> 2 si no, se considera Automatica -> 1
                                                    value = value.ToUpper().Contains("MANUAL") ? "2" : "1";
                                                    //switch (value.ToUpper())
                                                    //{
                                                    //    case "AUTOMATICA":
                                                    //        value = "1";
                                                    //        break;
                                                    //    case "MANUAL":
                                                    //        value = "2";
                                                    //        break;
                                                    //    default:
                                                    //        value = "1";
                                                    //        break;
                                                    //}
                                                    break;
                                                case "SubscriptionRatPlanChargeWBE":
                                                    index = 44;
                                                    value = fields[index];
                                                    break;
                                                case "EntityInvoiceNumber":
                                                    index = 45;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceSource":
                                                    index = 46;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceSourceID":
                                                    index = 47;
                                                    value = fields[index];
                                                    break;
                                                case "idCommodityCostumer":
                                                    index = 48;
                                                    value = fields[index];
                                                    var idCommodityCostumer = 0;
                                                    if (value.ToUpper() == "YES") idCommodityCostumer = 1;
                                                    if (value.ToUpper() == "NO") idCommodityCostumer = 2;
                                                    value = idCommodityCostumer.ToString();
                                                    break;
                                                case "InvoiceDescription":
                                                    index = 49;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceID":
                                                    index = 50;
                                                    value = fields[index];
                                                    break;
                                                case "AccountID":
                                                    index = 51;
                                                    value = fields[index];
                                                    break;
                                                case "idSegmento":
                                                    value = "2";//Constante
                                                    break;
                                                case "idInvoiceStatus":
                                                    value = "3";//Constante
                                                    break;
                                                case "idFamily":
                                                    var idFamily = 999;
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "única/unica/ónica/select/gas recurring/luz recurring"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 11;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "homix"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 3;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "protección hogar/proteccion gohar energía/proteccion hogar/hogar/stp"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 8;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "ok luz/okluz/oklu/luz"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        noContienePalabra = "asistencia"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra && !noContienePalabra) idFamily = 10;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "ok gas/okgas/okga/gas"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        noContienePalabra = "asistencia/caldera"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra && !noContienePalabra) idFamily = 9;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "rebilling/rectificativa/rectificativo"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 6;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "boiler/calentador/termo/solar/servicios básicos/air conditioning/valor residual/calefacción/calefaccion/aire acondicionado/	aire acondicionado/acondicionado/split/acondicionad/conditionin/calefaccio/calefacció/air/aire/básicos/basicos/photovoltaic/caldera/radiador/termo/solar/chaffoteaux/ferroli/haier/ssii/soluciones integrales/junker/wifi/vaillant/viessmann/servicios básicos/servicios basicos"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 7;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "a tu ritmo/juicepass/recarga/saba/bamsa/juicenet/perfil conductor/juice/pay per/rfid/pay per use/pack instalacion/pack instalación"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 4;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "technical/tecnico/técnico"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 5;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "seguro proteccion/seguro protección/seguro/protection"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 2;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "asistencia/servicio asistencia/maintenance/mantenimiento/reparacion/reparación"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 1;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        if (!string.IsNullOrEmpty(value = fields[10]) || (string.IsNullOrEmpty(value = fields[11]) && string.IsNullOrEmpty(value = fields[49])))
                                                            idFamily = 6;
                                                    }
                                                    value = idFamily.ToString();
                                                    break;
                                                case "filedate":
                                                    value = fechaFichero.ToString();//Constante
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
                                    //Tabla DRAFTS_VALIDATIONERROR
                                    var arrayValidationError = new object[datatableValidationError.Columns.Count];
                                    var valueValidationError = "";
                                    //Tabla DRAFTS
                                    for (int i = 0; i < datatableValidationError.Columns.Count; i++)
                                    {

                                        try
                                        {
                                            #region Lectura Columnas Fichero
                                            //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                            switch (datatableValidationError.Columns[i].ColumnName)
                                            {
                                                case "Invoice Date":
                                                    index = 0;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idBillingPeriod":
                                                    index = 5;
                                                    valueValidationError = fields[index];
                                                    //Obtención idBillingPeriod
                                                    var idChargeType = 0;
                                                    var idBillingPeriod = 4;
                                                    switch (valueValidationError)
                                                    {
                                                        case "Month":
                                                            idChargeType = 1;
                                                            break;
                                                        case "Specific Months":
                                                            idChargeType = 2;
                                                            break;
                                                        case "Annual":
                                                            idChargeType = 3;
                                                            break;
                                                    }

                                                    ratePlanName = fields[11];
                                                    invoiceDescription = fields[49];
                                                    familyREGEX = ratePlanName;
                                                    if (ratePlanName.Equals("disco") || string.IsNullOrEmpty(ratePlanName) || ratePlanName.Equals("Discount*"))
                                                        familyREGEX = invoiceDescription;

                                                    contienePalabra = "a tu ritmo/ a tu ritmo promo/saba/bamsa/a tu ritmo smart/a tu ritmo pro/rfid"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                    if (idChargeType == 1 && contienePalabra) idBillingPeriod = 5;

                                                    contienePalabra = "juicepass/a tu ritmo plus/a tur itmo plus/vinculado saba/vinculado bamsa/a tu ritmo smart/a tu ritmo pro/rfid/a tu ritmo flotas"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                    if (idChargeType == 1 && contienePalabra) idBillingPeriod = 1;
                                                    if (idChargeType == 2) idBillingPeriod = 5;
                                                    if (idChargeType != 1 && idChargeType != 2)
                                                    {
                                                        //Calcular Servince End Date - Service Start Date (en dias)
                                                        if (fields[2] != "" && fields[1] != "")
                                                        {
                                                            DateTime endDate = DateTime.ParseExact(fields[2], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                                            DateTime startDate = DateTime.ParseExact(fields[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                                            int diferenciaDias = (endDate - startDate).Days;
                                                            if (diferenciaDias <= 31)
                                                            {
                                                                idBillingPeriod = 1;
                                                            }
                                                            else
                                                            {
                                                                if (diferenciaDias <= 62)
                                                                {
                                                                    idBillingPeriod = 2;
                                                                }
                                                                else
                                                                {
                                                                    idBillingPeriod = 3;
                                                                }
                                                            }
                                                        }
                                                    }

                                                    valueValidationError = idBillingPeriod.ToString();
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 6;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "SubscriptionName":
                                                    index = 7;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "InvoiceCode":
                                                    index = 8;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idInvoiceCode":
                                                    index = 8;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "F1":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "F2":
                                                            valueValidationError = "2";
                                                            break;
                                                        case "R1":
                                                            valueValidationError = "3";
                                                            break;
                                                        case "R4":
                                                            valueValidationError = "4";
                                                            break;
                                                        case "R5":
                                                            valueValidationError = "5";
                                                            break;
                                                        default:
                                                            valueValidationError = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "Account Simplified Invoice":
                                                    index = 9;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "ReferenceToOriginalInvoice":
                                                    index = 10;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 11;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "RatePlanNamePARTHNER":
                                                    index = 11;
                                                    valueValidationError = fields[index];
                                                    if (valueValidationError.Length > 10) valueValidationError = fields[index].Substring(0, 9);
                                                    break;
                                                case "Account Number":
                                                    index = 12;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Due Date":
                                                    index = 13;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Account is Guest":
                                                    index = 14;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idIsGuest":
                                                    index = 14;
                                                    valueValidationError = fields[index];
                                                    var idIsGuest = 0;
                                                    if (valueValidationError.ToUpper() == "YES") idIsGuest = 1;
                                                    if (valueValidationError.ToUpper() == "NO") idIsGuest = 2;
                                                    valueValidationError = idIsGuest.ToString();
                                                    break;
                                                case "Account Identity Type":
                                                    index = 15;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Everest_Contract_Id":
                                                    index = 16;
                                                    valueValidationError = fields[index];
                                                    if (valueValidationError.Length > 200) valueValidationError = fields[index].Substring(0, 199);
                                                    break;
                                                case "Account Identity Number":
                                                    index = 17;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Contract State":
                                                    index = 18;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Account Name":
                                                    index = 19;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Amount":
                                                    if (row == 26278)
                                                    {
                                                        var a = 1;
                                                    }
                                                    index = 20;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Amount Without Tax":
                                                    index = 21;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Tax Amount":
                                                    index = 22;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Doxee Tax Rate":
                                                    index = 23;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Balance":
                                                    index = 24;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Currency":
                                                    index = 25;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice PURL":
                                                    index = 27;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idPURL":
                                                    index = 27;
                                                    valueValidationError = fields[index];
                                                    var idPurl = 0;
                                                    if (string.IsNullOrEmpty(valueValidationError))
                                                    {
                                                        idPurl = 0;
                                                    }
                                                    else
                                                    {
                                                        idPurl = 1;
                                                    }
                                                    valueValidationError = idPurl.ToString();
                                                    break;
                                                case "OtherPaymentMethodType":
                                                    index = 28;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idOtherPaymentMethodType":
                                                    index = 28;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "Bank Transfer":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "Barcode":
                                                            valueValidationError = "2";
                                                            break;
                                                        case "Commodity Bill":
                                                            valueValidationError = "3";
                                                            break;
                                                        case "Commodity Customer":
                                                            valueValidationError = "4";
                                                            break;
                                                        case "External Financing":
                                                            valueValidationError = "5";
                                                            break;
                                                        case "Sin identificar":
                                                            valueValidationError = "6";
                                                            break;
                                                        default:
                                                            valueValidationError = "6";
                                                            break;
                                                    }
                                                    break;
                                                case "Default Payment Method":
                                                    index = 29;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idCreditCardType":
                                                    index = 29;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "Visa":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "MasterCard":
                                                            valueValidationError = "2";
                                                            break;
                                                        default:
                                                            valueValidationError = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "Credit Card Mask Number":
                                                    index = 30;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Sold To Work Email":
                                                    index = 31;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Bill To First Name":
                                                    index = 32;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Updated Date":
                                                    index = 33;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Includes Usage":
                                                    index = 34;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Account Legal Entity":
                                                    index = 36;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idLegalEntity":
                                                    index = 36;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "Endesa X Servicios SL":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "Endesa Energia SAU":
                                                            valueValidationError = "2";
                                                            break;
                                                        default:
                                                            valueValidationError = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "Account EntityAccountNumber":
                                                    index = 37;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Validationerrorcode":
                                                    index = 38;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idValidationError":
                                                    index = 38;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "A001":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "A002":
                                                            valueValidationError = "2";
                                                            break;
                                                        case "A003":
                                                            valueValidationError = "3";
                                                            break;
                                                        case "A004":
                                                            valueValidationError = "4";
                                                            break;
                                                        case "A005":
                                                            valueValidationError = "5";
                                                            break;
                                                        case "A006":
                                                            valueValidationError = "6";
                                                            break;
                                                        case "A007":
                                                            valueValidationError = "7";
                                                            break;
                                                        case "A008":
                                                            valueValidationError = "8";
                                                            break;
                                                        case "A009":
                                                            valueValidationError = "9";
                                                            break;
                                                        case "A010":
                                                            valueValidationError = "10";
                                                            break;
                                                        case "A011":
                                                            valueValidationError = "11";
                                                            break;
                                                        case "A012":
                                                            valueValidationError = "12";
                                                            break;
                                                        case "A013":
                                                            valueValidationError = "13";
                                                            break;
                                                        case "A014":
                                                            valueValidationError = "14";
                                                            break;
                                                        case "A015":
                                                            valueValidationError = "15";
                                                            break;
                                                        case "M001":
                                                            valueValidationError = "101";
                                                            break;
                                                        case "M002":
                                                            valueValidationError = "102";
                                                            break;
                                                        case "M003":
                                                            valueValidationError = "103";
                                                            break;
                                                        case "P003":
                                                            valueValidationError = "1003";
                                                            break;
                                                        case "VAL-SII-0002":
                                                            valueValidationError = "1002";
                                                            break;
                                                        case "VAL-SII-0005":
                                                            valueValidationError = "1005";
                                                            break;
                                                        case "VAL-SII-0006":
                                                            valueValidationError = "1006";
                                                            break;
                                                        default:
                                                            valueValidationError = "9999";
                                                            break;
                                                    }
                                                    break;
                                                case "Invoice Created Date":
                                                    index = 39;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Default Payment Method Type":
                                                    index = 40;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idDefaultPaymentMethodType":
                                                    index = 40;
                                                    valueValidationError = fields[index];
                                                    switch (valueValidationError)
                                                    {
                                                        case "BankTransfer":
                                                            valueValidationError = "1";
                                                            break;
                                                        case "CreditCardReferenceTransaction":
                                                            valueValidationError = "2";
                                                            break;
                                                        case "Other":
                                                            valueValidationError = "3";
                                                            break;
                                                        default:
                                                            valueValidationError = "0";
                                                            break;
                                                    }
                                                    break;
                                                case "Account Invoice Delivery Preferences Print":
                                                    index = 41;
                                                    valueValidationError = fields[index];
                                                    if (valueValidationError.Length > 10) valueValidationError = fields[index].Substring(0, 9);
                                                    break;
                                                case "Account Invoice Delivery Preferences Email":
                                                    index = 42;
                                                    valueValidationError = fields[index];
                                                    if (valueValidationError.Length > 10) valueValidationError = fields[index].Substring(0, 9);
                                                    break;
                                                case "Invoice Validationerrordescription":
                                                    index = 43;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idValidacion":
                                                    index = 43;
                                                    valueValidationError = fields[index];
                                                    //Si contiene Manual -> 2 si no, se considera Automatica -> 1
                                                    valueValidationError = valueValidationError.ToUpper().Contains("MANUAL") ? "2" : "1";                                                    
                                                    //switch (valueValidationError.ToUpper())
                                                    //{
                                                    //    case "AUTOMATICA":
                                                    //        valueValidationError = "1";
                                                    //        break;
                                                    //    case "MANUAL":
                                                    //        valueValidationError = "2";
                                                    //        break;
                                                    //    default:
                                                    //        valueValidationError = "1";
                                                    //        break;
                                                    //}
                                                    break;
                                                case "Invoice EntityInvoiceNumber":
                                                    index = 45;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Source DRAFT":
                                                    index = 46;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice Is Commodity Customer":
                                                    index = 48;
                                                    valueValidationError = fields[index];
                                                    if (valueValidationError.Length > 5) valueValidationError = fields[index].Substring(0, 4);
                                                    break;
                                                case "idCommodityCostumer":
                                                    index = 48;
                                                    valueValidationError = fields[index];
                                                    var idCommodityCostumer = 0;
                                                    if (valueValidationError.ToUpper() == "YES") idCommodityCostumer = 1;
                                                    if (valueValidationError.ToUpper() == "NO") idCommodityCostumer = 2;
                                                    valueValidationError = idCommodityCostumer.ToString();
                                                    break;
                                                case "InvoiceDescription":
                                                    index = 49;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Invoice ID":
                                                    index = 50;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "Account ID":
                                                    index = 51;
                                                    valueValidationError = fields[index];
                                                    break;
                                                case "idSegmento":
                                                    valueValidationError = "2";//Constante
                                                    break;
                                                case "idInvoiceStatus":
                                                    valueValidationError = "3";//Constante
                                                    break;
                                                case "idFamily":
                                                    var idFamily = 999;
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "única/unica/ónica/select/gas recurring/luz recurring"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 11;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "homix"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 3;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "protección hogar/proteccion gohar energía/proteccion hogar/hogar/stp"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 8;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "ok luz/okluz/oklu/luz"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        noContienePalabra = "asistencia"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra && !noContienePalabra) idFamily = 10;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "ok gas/okgas/okga/gas"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        noContienePalabra = "asistencia/caldera"
                                                            .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra && !noContienePalabra) idFamily = 9;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "rebilling/rectificativa/rectificativo"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 6;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "boiler/calentador/termo/solar/servicios básicos/air conditioning/valor residual/calefacción/calefaccion/aire acondicionado/	aire acondicionado/acondicionado/split/acondicionad/conditionin/calefaccio/calefacció/air/aire/básicos/basicos/photovoltaic/caldera/radiador/termo/solar/chaffoteaux/ferroli/haier/ssii/soluciones integrales/junker/wifi/vaillant/viessmann/servicios básicos/servicios basicos"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 7;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "a tu ritmo/juicepass/recarga/saba/bamsa/juicenet/perfil conductor/juice/pay per/rfid/pay per use/pack instalacion/pack instalación"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 4;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "technical/tecnico/técnico"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 5;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "seguro proteccion/seguro protección/seguro/protection"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 2;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        contienePalabra = "asistencia/servicio asistencia/maintenance/mantenimiento/reparacion/reparación"
                                                        .Split('/').Any(familyREGEX.ToLower().Contains);
                                                        if (contienePalabra) idFamily = 1;
                                                    }
                                                    if (idFamily == 999)
                                                    {
                                                        if (!string.IsNullOrEmpty(value = fields[10]) || (string.IsNullOrEmpty(value = fields[11]) && string.IsNullOrEmpty(value = fields[49])))
                                                            idFamily = 6;
                                                    }
                                                    value = idFamily.ToString();
                                                    break;
                                                default:
                                                    index = 999;
                                                    value = "";
                                                    break;
                                            }
                                            #endregion
                                            #region Conversión Dato
                                            if (valueValidationError == "")
                                                arrayValidationError[i] = DBNull.Value;
                                            else
                                            {

                                                switch (datatableValidationError.Columns[i].DataType.ToString())
                                                {
                                                    case "System.DateTime":
                                                        {
                                                            arrayValidationError[i] = ConvierteDatoFecha(valueValidationError);
                                                        }
                                                        break;

                                                    case "System.String":
                                                        arrayValidationError[i] = valueValidationError;
                                                        break;

                                                    case "System.Decimal":
                                                        {
                                                            if (valueValidationError.StartsWith("-"))
                                                                valueValidationError = "-" + valueValidationError.Substring(1, valueValidationError.Length - 1).Trim();

                                                            valueValidationError = valueValidationError.Replace(",", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                                                            valueValidationError = valueValidationError.Replace(".", CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

                                                            while (valueValidationError.Split(',').Count() > 2)
                                                            {
                                                                var regex = new Regex(Regex.Escape(","));
                                                                valueValidationError = regex.Replace(valueValidationError, "", 1);
                                                            }

                                                            arrayValidationError[i] = double.Parse(valueValidationError, NumberStyles.Float, CultureInfo.CurrentCulture);
                                                        }
                                                        break;

                                                    case "System.Int32":
                                                        arrayValidationError[i] = int.Parse(valueValidationError);
                                                        break;

                                                    default:
                                                        throw new Exception($"Tipo {datatableValidationError.Columns[i].DataType.ToString()} no soportado en la lectura");
                                                }
                                            }
                                            #endregion
                                        }
                                        catch (Exception e)
                                        {
                                            throw new Exception($"Error durante la lectura y proceso de datos del fichero '{fileName}' en la linea {row} para el campo(BBDD): {datatableValidationError.Columns[i].ColumnName}. ERROR: {e.Message}");
                                        }
                                    }

                                    datatableValidationError.Rows.Add(arrayValidationError);
                                    if (datatableValidationError.Rows.Count > bufferSize)
                                    {
                                        database.Bulk(datatableValidationError, GetTableName2());
                                        datatableValidationError = datatableTemplateValidationError.Clone();
                                    }
                                }
                                line = sr.ReadLine();
                            }

                            if (datatable.Rows.Count > 0)
                                database.Bulk(datatable, GetTableName());
                            if (datatableValidationError.Rows.Count > 0)
                                database.Bulk(datatableValidationError, GetTableName2());

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

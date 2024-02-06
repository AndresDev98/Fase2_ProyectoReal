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
    public class ImportadorNotasCreditoSemanalB2B : Importador
    {
        public ImportadorNotasCreditoSemanalB2B(ILogger<Importador> logger) : base(logger)
        {
        }

        protected override char GetDelimited() => ';';

        protected override int GetFicheroTipoId() => 31;

        protected override string GetFicheroTipo() => "Notas Crédito B2B";

        protected override string GetHeader() => "Nota de crédito: Fecha de la nota de crédito;Nota de crédito: Número de nota de crédito;Nota de crédito: XC_CommodityStatus;Nota de crédito: Code;Cuenta: Identity Type;Cuenta: Número de cuenta;Factura: Rate Plan Name;Destinatario de factura: Estado o provincia;Cuenta: Nombre;Nota de crédito: Importe total sin impuestos;Factura: Doxee Tax Rate;Nota de crédito: Importe del impuesto;Nota de crédito: Importe total;Nota de crédito: Saldo;Factura: Amount Of Original Invoice;Factura: Tax Amount Of Original Invoice;Cuenta: Divisa;Nota de crédito: Transferido a contabilidad;Nota de crédito: Código del motivo;Nota de crédito: Comentarios;Nota de crédito: Reference to the original Invoice;Factura: Número de factura;Factura: Fecha de Operacion;Nota de crédito: Fecha de creación;Método de pago predeterminado: Tipo;Cuenta: otherPaymentMethodType;Cuenta: Is Commodity Customer;Factura: ConvergentBillingOperations;Factura: Emitido por;Nota de crédito: ID de autor de la cancelación;Nota de crédito: ID de creador;Nota de crédito: ID de autor de la emisión;Nota de crédito: ConvergentBillingOperations;Nota de crédito: PURL ;Destinatario de la venta: Estado o provincia;Cobro del plan de tarifa: Invoice Description";

        protected override string GetTableName() => "IN_NOTA_CREDITO_B2B";
        protected override string GetTableName2() => "";

        public void ExecuteNotasCreditoSemanalB2B(Database database, string fullName, DateTime fechaFichero, bool Transaction = false)
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

                                    for (int i = 0; i < datatable.Columns.Count; i++)
                                    {
                                        try
                                        {
                                            #region Lectura Columnas Fichero
                                            //A partir del nombre de columna, seleccionamos la posición (index) de la columna en el fichero y procesamos si es necesario.
                                            switch (datatable.Columns[i].ColumnName)
                                            {
                                                case "NCDate":
                                                    index = 0;
                                                    value = fields[index];
                                                    break;
                                                case "NCNumber":
                                                    index = 1;
                                                    value = fields[index];
                                                    break;
                                                case "XC_CommodityStatus":
                                                    index = 2;
                                                    value = fields[index];
                                                    break;
                                                case "NCCode":
                                                    index = 3;
                                                    value = fields[index];
                                                    break;
                                                case "AccountIdentityType":
                                                    index = 4;
                                                    value = fields[index];
                                                    break;
                                                case "AccountNumber":
                                                    index = 5;
                                                    value = fields[index];
                                                    break;
                                                case "RatePlanName":
                                                    index = 6;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceEstadoProvincia":
                                                    index = 7;
                                                    value = fields[index];
                                                    break;
                                                case "AccountName":
                                                    index = 8; value =
                                                        fields[index];
                                                    break;
                                                case "NCImporteTotalSinImpuestos":
                                                    index = 9;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "DoxeeTaxRate":
                                                    index = 10;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "NCImporteDelImpuesto":
                                                    index = 11;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "NCImporteTotal":
                                                    index = 12;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "NCSaldo":
                                                    index = 13;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "AmountOfOriginalInvoice":
                                                    index = 14;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "TaxAmountOfOriginalInvoice":
                                                    index = 15;
                                                    value = fields[index];
                                                    if (value != "") value = (Convert.ToDouble(value) * -1).ToString();
                                                    break;
                                                case "Divisa":
                                                    index = 16;
                                                    value = fields[index];
                                                    break;
                                                case "NCTrasferidoContabilidad":
                                                    index = 17;
                                                    value = fields[index];
                                                    break;
                                                case "NCCodigoMotivo":
                                                    index = 18;
                                                    value = fields[index];
                                                    break;
                                                case "NCComentarios":
                                                    index = 19;
                                                    value = fields[index];
                                                    break;
                                                case "NCReferenceOriginalInvoice":
                                                    index = 20;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceNumber":
                                                    index = 21;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceDate":
                                                    index = 22;
                                                    value = fields[index];
                                                    break;
                                                case "NCCreateDate":
                                                    index = 23;
                                                    value = fields[index];
                                                    break;
                                                case "MetodoPagoPredeterminado":
                                                    index = 24;
                                                    value = fields[index];
                                                    break;
                                                case "OtherPaymentMethodType":
                                                    index = 25;
                                                    value = fields[index];
                                                    break;
                                                case "idOtherPaymentMethodType":
                                                    index = 25;
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
                                                case "IsCommodityCustomer":
                                                    index = 26;
                                                    value = fields[index];
                                                    break;
                                                case "InvoiceConvergentBillingOperations":
                                                    index = 27;
                                                    value = fields[index];
                                                    break;
                                                case "FacturaEmitidoPor":
                                                    index = 28;
                                                    value = fields[index];
                                                    break;
                                                case "NCIDAutorCancelacion":
                                                    index = 29;
                                                    value = fields[index];
                                                    break;
                                                case "NCIDCreador":
                                                    index = 30;
                                                    value = fields[index];
                                                    break;
                                                case "NCIDAutorEmision":
                                                    index = 31;
                                                    value = fields[index];
                                                    break;
                                                case "NCConvergentBillingOperations":
                                                    index = 32;
                                                    value = fields[index];
                                                    break;
                                                case "NCPURL":
                                                    index = 33;
                                                    value = fields[index];
                                                    break;
                                                case "NCEstadoProvincia":
                                                    index = 34;
                                                    value = fields[index];
                                                    break;

                                                case "InvoiceDescription":
                                                    index = 35;
                                                    value = fields[index];
                                                    break;

                                                case "idFamily":
                                                    var ratePlanName = "";
                                                    var invoiceDescription = "";
                                                    var familyREGEX = "";
                                                    var idFamily = 999;//Por defecto
                                                    bool contienePalabra = false;
                                                    bool noContienePalabra = false;

                                                    ratePlanName = fields[6];
                                                    invoiceDescription = fields[35];

                                                    familyREGEX = ratePlanName;
                                                    if (ratePlanName.ToLower().Contains("disco") || string.IsNullOrEmpty(ratePlanName) || ratePlanName.ToLower().Contains("discount"))
                                                        familyREGEX = invoiceDescription;
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

                                                case "idSegmento":
                                                    value = "1";
                                                    break;

                                                case "idInvoiceStatus":
                                                    value = "9";
                                                    break;

                                                case "ficheroId":
                                                    value = ficheroId.ToString();
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

using Fase2_Global.Utilidades;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.FileIO;
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
    abstract public class Importador
    {
        protected ILogger Logger;

        protected Importador(ILogger<Importador> logger)
        {
            Logger = logger;
        }

        /// <summary>
        /// Eliminas Posibles comillas (delimitador de cadenas) que incluyen al crear el csv.
        /// </summary>
        /// <param name="linea"></param>
        /// <returns></returns>
        public string LimpiarLinea(string linea)
        {
            string limpio = "";

            if (linea != null && linea.Trim() != "")
            {
                limpio = linea.Trim('"');
                limpio = limpio.Replace("\";\"", ";");
            }

            return limpio;
        }

        /// <summary>
        /// Obtiene los campos de la linea a partir del delimitador indicado. Incluye campos con " para separar caracteres especiales
        /// </summary>
        /// <param name="linea"></param>
        /// <param name="delimitador"></param>
        /// <returns></returns>
        public string[] ObtieneCamposLinea(string linea, char delimitador)
        {
            TextFieldParser parser = new TextFieldParser(new StringReader(linea));
            parser.HasFieldsEnclosedInQuotes = true;
            parser.SetDelimiters(delimitador.ToString());

            string[] fields = new string[0]; //Inicializamos vacio

            try
            {
                while (!parser.EndOfData)
                {
                    fields = parser.ReadFields();
                }
                parser.Close();
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
            }

            return fields;
        }

        /// <summary>
        /// Convierte los datos recibidos en DateTime contemplando todas las posibilidades
        /// </summary>
        /// <param name="fecha"></param>
        /// <returns></returns>
        public object ConvierteDatoFecha(string fecha)
        {
            if (fecha == "0" || fecha == "00000000")
                return DBNull.Value;
            else
            {
                if (DateTime.TryParseExact(fecha, "yyyyMMdd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp) == true)
                    return temp;
                else
                {
                    if (DateTime.TryParseExact(fecha, "dd/MM/yyyy HH:mm:ss", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp2) == true)
                        return temp2;
                    else
                    {
                        if (DateTime.TryParseExact(fecha, "dd/MM/yyyy H:mm:ss", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp3) == true)
                            return temp3;
                        else
                        {
                            if (DateTime.TryParseExact(fecha, "d/MM/yyyy HH:mm:ss", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp10) == true)
                                return temp10;
                            else
                            {
                                if (DateTime.TryParseExact(fecha, "d/MM/yyyy H:mm:ss", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp11) == true)
                                    return temp11;
                                else
                                {
                                    if (DateTime.TryParseExact(fecha, "dd/MM/yyyy HH:mm", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp4) == true)
                                        return temp4;
                                    else
                                    {
                                        if (DateTime.TryParseExact(fecha, "d/MM/yyyy HH:mm", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp5) == true)
                                            return temp5;
                                        else
                                        {
                                            if (DateTime.TryParseExact(fecha, "dd/MM/yyyy H:mm", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp6) == true)
                                                return temp6;
                                            else
                                            {
                                                if (DateTime.TryParseExact(fecha, "d/MM/yyyy H:mm", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp7) == true)
                                                    return temp7;
                                                else
                                                {
                                                    if (DateTime.TryParseExact(fecha, "dd/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp8) == true)
                                                        return temp8;
                                                    else
                                                    {
                                                        if (DateTime.TryParseExact(fecha, "d/MM/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp9) == true)
                                                            return temp9;
                                                        else
                                                        {
                                                            if (DateTime.TryParseExact(fecha, "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime temp13) == true)
                                                                return temp13;
                                                            else
                                                            {
                                                                return DBNull.Value;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        protected virtual int GetBufferSize() => 100000;

        protected virtual char GetDelimited() => ';';

        abstract protected int GetFicheroTipoId();

        abstract protected string GetFicheroTipo();

        protected virtual string GetHeader() => null;

        abstract protected string GetTableName();
        abstract protected string GetTableName2();

        virtual public string GetTableBBDD()
        {
            return GetTableName();
        }
        virtual public string GetTableBBDD2()
        {
            return GetTableName2();
        }
    }
}
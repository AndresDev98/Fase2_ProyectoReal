using Fase2_Global.Models.EnumsModels;
using Fase2_Global.Models.FamilyModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System;
using System.Collections.Generic;

namespace Fase2_Global.Utilidades
{
    internal class FamilyManager
    {
        
        protected ILogger Logger;        
        protected IConfiguration Configuration;
        
        public FamilyManager(ILogger<FamilyManager> logger, IConfiguration configuration)
        {
            this.Logger = logger;
            Configuration = configuration;
        }

        public async void ExecuteSetFamilyByKeyWords(Database database, TypeJobProcessC type)
        {
            
            var result = await ReadAndBulkInsertAsync(database,type);
            if (!result.Item1)
                throw new Exception (result.Item2);

        }

        private async Task<(bool, string)> ReadAndBulkInsertAsync(Database database, TypeJobProcessC type)
        {
            bool isOK = true;
            string message = string.Empty;

            try
            {
                string connectionString = Configuration.GetConnectionString("DefaultConnection");
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Leer los registros de la tabla T1 en bloques de 10,000
                    string selectQuery = GetSQLByType(type);
                    using (SqlCommand command = new SqlCommand(selectQuery, connection))
                    {
                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
                        {
                            // Configurar la conexión y opciones para la inserción masiva
                            using (SqlConnection destinationConnection = new SqlConnection(connectionString))
                            {
                                await destinationConnection.OpenAsync();
                                using (SqlTransaction transaction = destinationConnection.BeginTransaction())
                                {
                                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.Default, transaction))
                                    {
                                        
                                        database.Execute("truncate table result_familiy");
                                        var targetModel = database.GetData("select top 0 * from result_family");

                                        bulkCopy.DestinationTableName = "result_familiy";
                                        bulkCopy.BulkCopyTimeout = 3000; // Establecer un tiempo de espera para la operación

                                        // Configurar el mapeo de columnas si es necesario
                                        // bulkCopy.ColumnMappings.Add("ColumnaOrigen", "ColumnaDestino");

                                        // Variables para el bloque de lectura
                                        int batchSize = 100000;
                                        int totalCount = 0;
                                        bool hasRows = false;

                                        do
                                        {
                                            // Leer el siguiente bloque de registros
                                            hasRows = false;
                                            using (DataTable dataTable = targetModel.Clone())
                                            {

                                                //for (int i = 0; i < reader.FieldCount; i++)
                                                //{
                                                //    dataTable.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                                                //}

                                                for (int i = 0; i < batchSize; i++)
                                                {
                                                    if (await reader.ReadAsync())
                                                    {
                                                        string invoiceNumber = reader.GetString("InvoiceNumber");
                                                        int idSegmento = reader.GetInt32("idSegmento");
                                                        int idFamily = reader.GetInt32("idFamily");
                                                        string KEY_WORD = reader.GetString("KEY_WORD");
                                                        string PARTHNER = reader.GetString("PARTHNER");

                                                        int idFamilyKey = 0;
                                                        
                                                        hasRows = true;
                                                        totalCount++;

                                                        DataRow dataRow = dataTable.NewRow();
                                                        dataRow[0] = invoiceNumber; 
                                                        dataRow[1] = idSegmento;
                                                        dataRow[2] = idFamilyKey;
                                                        dataTable.Rows.Add(dataRow);
                                                    }
                                                }

                                                // Insertar el bloque de registros en la tabla T2
                                                if (hasRows)
                                                {
                                                    await bulkCopy.WriteToServerAsync(dataTable);
                                                }
                                            }
                                        }
                                        while (hasRows);

                                        await transaction.CommitAsync();
                                        Console.WriteLine($"Se insertaron {totalCount} registros en la tabla T2.");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                isOK = false;
                message = e.Message;

                Logger.LogError(e, e.Message);
            }

            return (isOK, message);
        }


        public async Task BulkInsertDataAsync01(TypeJobProcessC type)
        {
            string connectionString = Configuration.GetConnectionString("DefaultConnection");
            using (SqlConnection sourceConnection = new SqlConnection(connectionString))
            {
                await sourceConnection.OpenAsync();

                // Obtener los datos de la tabla origen
                string selectQuery = GetSQLByType(type);
                using (SqlCommand command = new SqlCommand(selectQuery, sourceConnection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        reader.Read();
                        // Configurar la conexión y opciones para la inserción masiva
                        using (SqlConnection destinationConnection = new SqlConnection(connectionString))
                        {
                            await destinationConnection.OpenAsync();
                            using (SqlTransaction transaction = destinationConnection.BeginTransaction())
                            {
                                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.Default, transaction))
                                {
                                    bulkCopy.DestinationTableName = "TablaDestino";
                                    bulkCopy.BulkCopyTimeout = 60; // Establecer un tiempo de espera para la operación

                                    // Configurar el mapeo de columnas si es necesario
                                    // bulkCopy.ColumnMappings.Add("ColumnaOrigen", "ColumnaDestino");

                                    try
                                    {
                                        // Realizar la inserción masiva de manera asíncrona
                                        await bulkCopy.WriteToServerAsync(reader);
                                        await transaction.CommitAsync();
                                    }
                                    catch (Exception ex)
                                    {
                                        await transaction.RollbackAsync();
                                        // Manejar el error
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        public void ExecuteSetFamilyByKeyWords01(Database database, TypeJobProcessC type)
        {
            var families = database.GetData("select * from dim_familia");
            var wordKeys = database.GetData("select * from dim_palabra_clave");

            if (families.Rows.Count == 0)
            {
                Logger.LogInformation("No existen reglas de negocio de Famialias");
                return;
            }
            if (wordKeys.Rows.Count == 0)
            {
                Logger.LogInformation("No existen reglas de negocio de Palabras Claves");
                return;

            }
            string job = "";
            switch (type)
            {
                case TypeJobProcessC.JobProcessCDiary:
                    job = "nuevas facturas emitidas a calcular su Famlia en el proceso diario";
                    break;
                case TypeJobProcessC.JobProcessCWeekly:
                    job = "nuevas facturas emitidas a calcular su Familia en el proceso semanal";
                    break;
                case TypeJobProcessC.JobProcessCMonthly:
                    job = "nuevas subscripciones a calcualor su Familia en el proceso mensual";
                    break;
            }

            var reader = database.ExecuteReader(GetSQLByType(type));
            if (reader == null) 
            { 
                Logger.LogInformation($"No existen {job}");
                return;
            }

            List<ResultModel> results = new List<ResultModel>();

            while (reader.Read())
            {
                string invoiceNumber = reader.GetString("InvoiceNumber");
                int idSegmento = reader.GetInt32("idSegmento");
                int idFamily = reader.GetInt32("idFamily");
                string KEY_WORD = reader.GetString("KEY_WORD");
                string PARTHNER = reader.GetString("PARTHNER");

                //int idFamilyOK = database.GetData("select * from ")


            }


        }


        private string GetSQLByType(TypeJobProcessC type)
        {
            string sql;
            string columns="";
            string table="";
            string COLUMNS_DIARY_WEEKLY = "InvoiceNumber, idSegmento, idFamily, ISNULL(RatePlanName,InvoiceDescription) AS KEY_WORD, RatePlanNamePARTHNER AS PARTHNER";
            string COLUMNS_MONTHLY = "InvoiceNumber, idSegmento, idFamily, RatePlanName AS KEY_WORD, '' AS PARTHNER";

            switch (type)
            {
                case TypeJobProcessC.JobProcessCDiary:  
                case TypeJobProcessC.JobProcessCWeekly:
                    table = (type == TypeJobProcessC.JobProcessCDiary)? "IN_FACTURAS_EMITIDAS_B2C": "IN_FACTURAS_EMITIDAS_B2B";
                    columns = COLUMNS_DIARY_WEEKLY;
                    break;                    
                case TypeJobProcessC.JobProcessCMonthly:
                    table = "IN_SUBSCRIPCIONES_B2C";
                    columns = COLUMNS_MONTHLY;
                    break; 
            }

            sql = $"select {columns} from {table} where IdFamily = 888";
            return sql;
        }


    }
}

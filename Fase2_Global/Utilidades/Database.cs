using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SqlBulkTools;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Fase2_Global.Utilidades
{
    public class Database : IDisposable
    {
        private SqlConnection _Connection;
        private string ConnectionString;
        private ILogger Logger;
        private SqlTransaction Transaction = null;

        public Database(IConfiguration configuration, ILogger<Database> logger)
        {
            this.Logger = logger;

            ConnectionString = configuration.GetConnectionString("DefaultConnection");
        }

        private SqlConnection Connection
        {
            get
            {
                try
                {
                    if (_Connection == null)
                    {
                        Logger.LogInformation("Abriendo conexión con la base de datos");
                        _Connection = new SqlConnection(ConnectionString);
                        _Connection.Open();
                    }
                    return _Connection;
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Error al conectar con la base de datos");
                    throw e;
                }
            }
        }

        public SqlTransaction BeginTransaction()
        {
            Open();
            if (Transaction != null)
                throw new Exception("Ya existe una transacción abierta");
            Transaction = Connection.BeginTransaction();
            return Transaction;
        }

        public int ExecuteQueryInteger(string query)
        {
            var command = GetCommand(query);

            try
            {
                Logger.LogDebug($"Ejecutando consulta {formatQuery(query)}");
                Open();
                return (int)command.ExecuteScalar();
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
            finally
            {
            }
        }

        public void Bulk<T>(List<T> rows, string tableName, Expression<Func<T, object>> GetIdentity, SqlBulkTools.Enumeration.ColumnDirectionType directionType) where T : class
        {
            Open();

            //var b = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, Transaction);
            //b.BulkCopyTimeout = 3000;
            //b.DestinationTableName = tableName;

            var bulk = new BulkOperations();

            bulk.Setup<T>()
                .ForCollection(rows)

                .WithTable(tableName)
                .AddAllColumns()

                .BulkInsert()

                .SetIdentityColumn(GetIdentity, directionType)

                .Commit(Transaction.Connection, Transaction);
        }

        public void Bulk(DataTable dataTable, string tableName)
        {
            Open();

            var b = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, Transaction);
            b.BulkCopyTimeout = 3000;
            b.DestinationTableName = tableName;
            b.NotifyAfter = dataTable.Rows.Count / 10;
            b.SqlRowsCopied += (sender, e) => Console.Write(".");

            try
            {
                Logger.LogInformation($"Guardando {dataTable.Rows.Count} registros en la tabla {tableName}");
                b.WriteToServer(dataTable);
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al guardar registros en {tableName}");
                throw e;
            }
            finally
            {
                Console.Write("\n\r");
            }
        }

        public void CommitTransaction()
        {
            if (Transaction == null)
                throw new Exception("No existe ninguna transacción abierta");
            Transaction.Commit();
            Transaction = null;
        }

        public void Dispose()
        {
            if (Connection.State == ConnectionState.Open)
            {
                Logger.LogInformation("Cerrando conexión con la base de datos");
                Connection.Close();                
            }
        }

        public string Execute(string query, string output)
        {
            var command = GetCommand(query);

            command.CommandType = CommandType.StoredProcedure;

            var parametro = new SqlParameter($"@{output}", SqlDbType.NVarChar, 200);
            parametro.Direction = ParameterDirection.Output;
            command.Parameters.Add(parametro);

            try
            {
                Logger.LogDebug($"Ejecutando consulta {formatQuery(query)}");
                Open();
                command.ExecuteNonQuery();
                return (string)parametro.Value;
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
            finally
            {
            }
        }

        public void Execute(string query)
        {
            var command = GetCommand(query);

            try
            {
                if (query.Length < 10)
                    Logger.LogInformation($"Ejecutando consulta {formatQuery(query)}");

                Open();
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
            finally
            {
            }
        }

        public int ExecuteInsert(string query)
        {
            query += ";select SCOPE_IDENTITY()";
            var command = GetCommand(query);

            try
            {
                Logger.LogDebug($"Ejecutando consulta {formatQuery(query)}");
                Open();
                return (int)(decimal)command.ExecuteScalar();
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
            finally
            {
            }
        }

        public SqlDataReader ExecuteReader(string query)
        {
            return GetCommand(query).ExecuteReader();
        }

        public T ExecuteScalar<T>(string query)
        {
            var command = GetCommand(query);

            try
            {
                Logger.LogDebug($"Ejecutando consulta {formatQuery(query)}");
                Open();
                return (T)command.ExecuteScalar();
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
            finally
            {
            }
        }

        public SqlCommand GetCommand(string query)
        {
            var command = new SqlCommand(query, Connection, Transaction);
            command.CommandTimeout = 5600;
            return command;
        }

        public DataTable GetData(string query)
        {
            return GetData(GetCommand(query));
        }

        public DataTable GetData(SqlCommand command)
        {
            try
            {
                if (command.CommandText.Length < 1000)
                    Logger.LogDebug($"Ejecutando consulta {formatQuery(command.CommandText)}");
                Open();
                SqlDataAdapter adapter = new SqlDataAdapter(command);
                DataTable result = new DataTable();

                adapter.Fill(result);
                return result;
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Error al ejecutar consulta");
                throw e;
            }
        }

        public void Open()
        {
            if (Connection.State != ConnectionState.Open)
                Connection.Open();
        }

        public void RollbackTransaction()
        {
            if (Transaction == null)
                throw new Exception("No existe ninguna transacción abierta");
            Transaction.Rollback();
            Transaction = null;
        }

        private void B_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e) => throw new NotImplementedException();

        private string formatQuery(string query)
        {
            try
            {
                query = query.Replace("\t", " ").Replace("\r", " ").Replace("\n", " ");
                while (query.IndexOf("  ") >= 0)
                    query = query.Replace("  ", " ");

                return query;
            }
            catch (Exception E)
            {
                Logger.LogError($"Error al mostrar query (no al ejecutarla). Error menor: {E.Message}");
                return "";
            }
        }
    }
}
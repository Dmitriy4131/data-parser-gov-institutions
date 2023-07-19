using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataParserGovInstitutions
{
    public class DataProcessor
    {
        public void InsertOrUpdateData(List<Data> allData, string lastUpdateFrom, string lastUpdateTo)
        {
            if (allData.Count == 0)
            {
                return;
            }

            DateTime fromDate = DateTime.Parse(lastUpdateFrom);
            DateTime toDate = DateTime.Parse(lastUpdateTo);

            CreateTable();

            using (var connection = GetConnection())
            {
                if (CheckDataExists(connection, fromDate, toDate))
                {
                    UpdateData(allData, connection, fromDate, toDate);
                }
                else
                {
                    InsertData(allData, connection, fromDate, toDate);
                }
            }
        }

        public void CreateTable()
        {
            string tableName = "test";

            using (var connection = GetConnection())
            {
                string checkTableQuery = $"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}')";

                using (var checkCommand = new NpgsqlCommand(checkTableQuery, connection))
                {
                    bool tableExists = Convert.ToBoolean(checkCommand.ExecuteScalar());

                    if (tableExists)
                    {
                        Console.WriteLine($"Таблица '{tableName}' уже существует.");
                        return;
                    }
                }

                string createTableQuery = "CREATE TABLE IF NOT EXISTS test (" +
                    "content json," +
                    "total_pages bigint," +
                    "last boolean," +
                    "total_elements_integer bigint," +
                    "total_elements bigint," +
                    "first boolean," +
                    "number_of_elements bigint," +
                    "size bigint," +
                    "number bigint," +
                    "sort text NULL," +
                    "last_update_from date," +
                    "last_update_to date)";

                using (var createCommand = new NpgsqlCommand(createTableQuery, connection))
                {
                    createCommand.ExecuteNonQuery();
                }
            }

            Console.WriteLine($"Таблица {tableName} успешно создана");
        }

        public bool CheckDataExists(NpgsqlConnection connection, DateTime lastUpdateFrom, DateTime lastUpdateTo)
        {
            string checkQuery = "SELECT COUNT(*) FROM test WHERE last_update_from = @lastUpdateFrom AND last_update_to = @lastUpdateTo";

            using (var command = new NpgsqlCommand(checkQuery, connection))
            {
                command.Parameters.AddWithValue("@lastUpdateFrom", lastUpdateFrom);
                command.Parameters.AddWithValue("@lastUpdateTo", lastUpdateTo);

                int count = Convert.ToInt32(command.ExecuteScalar());

                return count > 0;
            }
        }

        public NpgsqlConnection GetConnection()
        {
            string connectionString = "Host=127.0.0.1;Port=5432;Database=institutions;Username=postgres;Password=root";
            var connection = new NpgsqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        public void InsertData(List<Data> allData, NpgsqlConnection connection, DateTime fromDate, DateTime toDate)
        {
            var insertQuery = new StringBuilder();

            insertQuery.AppendLine("INSERT INTO test (content, total_pages, last, total_elements_integer, total_elements, first, number_of_elements, size, number, sort, last_update_from, last_update_to)");
            insertQuery.AppendLine("VALUES");

            for (int i = 0; i < allData.Count; i++)
            {

                insertQuery.Append($"(@content{i}, @totalPages{i}, @last{i}, @totalElementsInteger{i}, @totalElements{i}, @first{i}, @numberOfElements{i}, @size{i}, @number{i}, @sort{i}, @lastUpdateFrom{i}, @lastUpdateTo{i})");

                if (i < allData.Count - 1)
                {
                    insertQuery.Append(",");
                }
            }

            using (var command = new NpgsqlCommand(insertQuery.ToString(), connection))
            {
                for (int i = 0; i < allData.Count; i++)
                {
                    Data data = allData[i];

                    command.Parameters.AddWithValue($"@content{i}", NpgsqlTypes.NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(data.content));
                    command.Parameters.AddWithValue($"@totalPages{i}", data.totalPages);
                    command.Parameters.AddWithValue($"@last{i}", data.last);
                    command.Parameters.AddWithValue($"@totalElementsInteger{i}", data.totalElementsInteger);
                    command.Parameters.AddWithValue($"@totalElements{i}", data.totalElements);
                    command.Parameters.AddWithValue($"@first{i}", data.first);
                    command.Parameters.AddWithValue($"@numberOfElements{i}", data.numberOfElements);
                    command.Parameters.AddWithValue($"@size{i}", data.size);
                    command.Parameters.AddWithValue($"@number{i}", data.number);

                    if (data.sort != null)
                    {
                        command.Parameters.AddWithValue($"@sort{i}", data.sort);
                    }
                    else
                    {
                        command.Parameters.AddWithValue($"@sort{i}", DBNull.Value);
                    }

                    command.Parameters.AddWithValue($"@lastUpdateFrom{i}", fromDate);
                    command.Parameters.AddWithValue($"@lastUpdateTo{i}", toDate);
                }

                int rowsAffected = command.ExecuteNonQuery();

                if (rowsAffected > 0)
                {
                    Console.WriteLine($"Данные для диапазона дат {fromDate} - {toDate} успешно добавлены в базу данных.");
                }
                else
                {
                    Console.WriteLine($"Ошибка при записи данных для диапазона дат {fromDate} - {toDate}.");
                }
            }
        }

        public void UpdateData(List<Data> allData, NpgsqlConnection connection, DateTime fromDate, DateTime toDate)
        {
            try
            {
                foreach (var data in allData)
                {
                    StringBuilder updateQuery = new StringBuilder();

                    updateQuery.AppendLine("UPDATE test SET");
                    updateQuery.AppendLine("content = @content,");
                    updateQuery.AppendLine("total_pages = @totalPages,");
                    updateQuery.AppendLine("last = @last,");
                    updateQuery.AppendLine("total_elements_integer = @totalElementsInteger,");
                    updateQuery.AppendLine("total_elements = @totalElements,");
                    updateQuery.AppendLine("first = @first,");
                    updateQuery.AppendLine("number_of_elements = @numberOfElements,");
                    updateQuery.AppendLine("size = @size,");
                    updateQuery.AppendLine("number = @number,");
                    updateQuery.AppendLine("sort = @sort");
                    updateQuery.AppendLine("WHERE last_update_from = @fromDate AND last_update_to = @toDate");

                    using (var command = new NpgsqlCommand(updateQuery.ToString(), connection))
                    {
                        command.Parameters.AddWithValue("@content", NpgsqlTypes.NpgsqlDbType.Jsonb, JsonConvert.SerializeObject(data.content));
                        command.Parameters.AddWithValue("@totalPages", data.totalPages);
                        command.Parameters.AddWithValue("@last", data.last);
                        command.Parameters.AddWithValue("@totalElementsInteger", data.totalElementsInteger);
                        command.Parameters.AddWithValue("@totalElements", data.totalElements);
                        command.Parameters.AddWithValue("@first", data.first);
                        command.Parameters.AddWithValue("@numberOfElements", data.numberOfElements);
                        command.Parameters.AddWithValue("@size", data.size);
                        command.Parameters.AddWithValue("@number", data.number);
                        if (data.sort != null)
                        {
                            command.Parameters.AddWithValue($"@sort", data.sort);
                        }
                        else
                        {
                            command.Parameters.AddWithValue($"@sort", DBNull.Value);
                        }

                        command.Parameters.AddWithValue("@fromDate", fromDate);
                        command.Parameters.AddWithValue("@toDate", toDate);

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            Console.WriteLine($"Данные для диапазона дат {fromDate} - {toDate}  успешно обновлены в базе данных.");
                        }
                        else
                        {
                            Console.WriteLine($"Ошибка при обновлении данных для диапазона дат {fromDate} - {toDate}.");
                        }
                    }
                }
            }
            catch (Npgsql.PostgresException ex)
            {
                    Console.WriteLine($"Ошибка при выполнении SQL-запроса: {ex.Message}");
            }
            finally
            {
                connection.Close();
            }
        }
    }
}

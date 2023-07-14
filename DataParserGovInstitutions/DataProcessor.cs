using Newtonsoft.Json;
using Npgsql;
using System;


namespace DataParserGovInstitutions
{
    class DataProcessor
    {
        public void InsertOrUpdateData(Data data, string lastUpdateFrom, string lastUpdateTo)
        {
            using (var connection = GetConnection())
            {
                string insertQuery = "INSERT INTO test (content, total_pages, last, total_elements_integer, total_elements, first, number_of_elements, size, number, sort, last_update_from, last_update_to) " +
                                     "VALUES (@content,@totalPages, @last, @totalElementsInteger, @totalElements, @first, @numberOfElements, @size, @number ,@sort, @lastUpdateFrom, @lastUpdateTo)";

                string updateQuery = "UPDATE test SET content = @content, total_pages = @totalPages, last = @last, total_elements_integer = @totalElementsInteger, total_elements = @totalElements, " +
                                 "first = @first, number_of_elements = @numberOfElements, size = @size, number = @number, sort = @sort " +
                                 "WHERE last_update_from = @lastUpdateFrom AND last_update_to = @lastUpdateTo";

                using (var command = new NpgsqlCommand(insertQuery, connection))
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
                        command.Parameters.AddWithValue("@sort", data.sort);
                    }
                    else
                    {
                        command.Parameters.AddWithValue("@sort", DBNull.Value);
                    }

                    DateTime fromDate = DateTime.Parse(lastUpdateFrom);
                    DateTime toDate = DateTime.Parse(lastUpdateTo);

                    command.Parameters.AddWithValue("@lastUpdateFrom", fromDate);
                    command.Parameters.AddWithValue("@lastUpdateTo", toDate);

                    int rowsAffected = command.ExecuteNonQuery();

                    if (rowsAffected == 0)
                    {
                        // Если не была выполнена вставка, выполняем обновление
                        command.CommandText = updateQuery;
                        rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            Console.WriteLine($"Ошибка при записи данных для диапазона дат {lastUpdateFrom} - {lastUpdateTo}.");
                        }
                        else
                        {
                            Console.WriteLine($"Данные для диапазона дат {lastUpdateFrom} - {lastUpdateTo} успешно обновлены в базе данных.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Данные для диапазона дат {lastUpdateFrom} - {lastUpdateTo} успешно добавлены в базу данных.");
                    }
                }
            }
        }

        public NpgsqlConnection GetConnection()
        {
            string connectionString = "Host=127.0.0.1;Port=5432;Database=institutions;Username=postgres;Password=root";
            var connection = new NpgsqlConnection(connectionString);
            connection.Open();
            return connection;
        }
    }
}

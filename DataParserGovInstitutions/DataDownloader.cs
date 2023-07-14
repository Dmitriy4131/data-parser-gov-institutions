using Newtonsoft.Json;
using Npgsql;
using RestSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace DataParserGovInstitutions
{
    class DataDownloader
    {
        private readonly RestClient client;

        public DataDownloader()
        {
            client = new RestClient();
        }

        public List<Data> DownloadAllDataAndCreateZipArchive(string lastUpdateFrom, string lastUpdateTo)
        {
            List<Data> allData = new List<Data>();
            int page = 0;
            int pageSize = 100;

            string zipPath = "data.zip";
            using (var zipArchive = ZipFile.Open(zipPath, ZipArchiveMode.Create))
            {
                CreateTable();

                if (CheckDataExists(lastUpdateFrom, lastUpdateTo))
                {
                    Console.WriteLine($"Данные для диапазона дат {lastUpdateFrom} - {lastUpdateTo} уже существуют в базе данных.");
                    return allData;
                }

                while (true)
                {
                    string url = $"https://bus.gov.ru/public-rest/api/epbs/fap?lastUpdateFrom={lastUpdateFrom}&lastUpdateTo={lastUpdateTo}&page={page}&size={pageSize}";

                    RestRequest request = new RestRequest(url, Method.Get);

                    RestResponse response = client.Execute(request);

                    if (response.StatusCode != System.Net.HttpStatusCode.OK)
                    {
                        Console.WriteLine($"Ошибка при загрузке страницы {page}");
                        break;
                    }

                    string jsonData = response.Content;

                    string filePath = $"{page}.json";
                    File.WriteAllText(filePath, jsonData);

                    zipArchive.CreateEntryFromFile(filePath, Path.GetFileName(filePath));

                    Console.WriteLine($"Файл {filePath} успешно создан и добавлен в zip-архив.");

                    File.Delete(filePath);

                    Console.WriteLine($"Файл {filePath} успешно удален.");

                    Data responseData = JsonConvert.DeserializeObject<Data>(jsonData);
                    allData.Add(responseData);

                    if (responseData.content.Length < pageSize)
                        break;

                    page++;
                }
            }

            return allData;
        }

        public bool CheckDataExists(string lastUpdateFrom, string lastUpdateTo)
        {
            using (var connection = GetConnection())
            {
                string checkQuery = "SELECT COUNT(*) FROM test WHERE last_update_from = @lastUpdateFrom AND last_update_to = @lastUpdateTo";

                using (var command = new NpgsqlCommand(checkQuery, connection))
                {
                    DateTime fromDate = DateTime.Parse(lastUpdateFrom);
                    DateTime toDate = DateTime.Parse(lastUpdateTo);

                    command.Parameters.AddWithValue("@lastUpdateFrom", fromDate);
                    command.Parameters.AddWithValue("@lastUpdateTo", toDate);

                    int count = Convert.ToInt32(command.ExecuteScalar());

                    return count > 0;
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

        public void CreateTable()
        {
            using (var connection = GetConnection())
            {
                string createTableQuery = "CREATE TABLE IF NOT EXISTS test (" +
                "id SERIAL PRIMARY KEY," +
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

                using (var command = new NpgsqlCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
            Console.WriteLine("Таблица успешно создана");
        }
    }
}

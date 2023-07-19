using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;

namespace DataParserGovInstitutions
{
    public class DataDownloader
    {
        private readonly HttpClient httpClient;

        public DataDownloader()
        {
            httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 YaBrowser/23.5.4.674 Yowser/2.5 Safari/537.36");
        }

        public async Task<List<Data>> DownloadAllDataAndCreateZipArchive(string lastUpdateFrom, string lastUpdateTo)
        {
            List<Data> allData = new List<Data>();
            int page = 0;
            int pageSize = 100;

            string zipPath = $"{lastUpdateFrom}-{lastUpdateTo}.zip";

            if (File.Exists(zipPath))
            {            
                using (var zipArchive = ZipFile.Open(zipPath, ZipArchiveMode.Update))
                {
                    var entriesToDelete = new List<ZipArchiveEntry>();

                    foreach (var entry in zipArchive.Entries)
                    {
                        entriesToDelete.Add(entry);
                    }

                    foreach (var entry in entriesToDelete)
                    {
                        entry.Delete();
                    }

                    await ProcessingPageData(lastUpdateFrom, lastUpdateTo, page, pageSize, allData, zipArchive);
                }
                return allData;
            }

            using (var zipArchive = ZipFile.Open(zipPath, ZipArchiveMode.Create))
            {
                await ProcessingPageData(lastUpdateFrom, lastUpdateTo, page, pageSize, allData, zipArchive);
            }
            return allData;
        }

        public async Task ProcessingPageData(string lastUpdateFrom, string lastUpdateTo, int page, int pageSize, List<Data> allData, ZipArchive zipArchive)
        {
            string url = $"https://bus.gov.ru/public-rest/api/epbs/fap?lastUpdateFrom={lastUpdateFrom}&lastUpdateTo={lastUpdateTo}&page={page}&size={pageSize}";

            try
            {
                var httpResponse = await httpClient.GetAsync(url);

                if (httpResponse.IsSuccessStatusCode)
                {
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    Data responseData = JsonConvert.DeserializeObject<Data>(responseContent);
                    allData.Add(responseData);

                    string jsonData = JsonConvert.SerializeObject(responseData);
                    string filePath = $"{page}.json";

                    using (var entryStream = zipArchive.CreateEntry(filePath).Open())
                    using (var writer = new StreamWriter(entryStream))
                    {
                        await writer.WriteAsync(jsonData);
                    }

                    Console.WriteLine($"Файл {filePath} успешно создан и добавлен в zip-архив.");

                    if (responseData.content.Length < pageSize)
                        return;

                    page++;
                    await ProcessingPageData(lastUpdateFrom, lastUpdateTo, page, pageSize, allData, zipArchive);
                }
                else
                {
                    Console.WriteLine($"Ошибка получения данных со страницы {page}. HTTP-код ответа: {(int)httpResponse.StatusCode} {httpResponse.ReasonPhrase}");
                }
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine($"Ошибка получения данных со страницы {page} и {ex.Message}");
            }
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataParserGovInstitutions
{
    internal class Program
    {
        static async Task Main()
        {
            string lastUpdateFrom = "04.12.2019";
            string lastUpdateTo = "10.12.2019";

            DataDownloader downloader = new DataDownloader();
            List<Data> allData = await downloader.DownloadAllDataAndCreateZipArchive(lastUpdateFrom, lastUpdateTo);

            DataProcessor processor = new DataProcessor();
            processor.InsertOrUpdateData(allData, lastUpdateFrom, lastUpdateTo);

            Console.WriteLine("Задание выполнено.");
            Console.ReadKey();
        }
    }
}

using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Assert = NUnit.Framework.Assert;

namespace DataParserGovInstitutionsTests
{
    public class DataDownloaderTests
    {
        [Test]
        public async Task WebsiteIsAccessibleAndReturnsSuccessStatusCode()
        {
            // Arrange
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 YaBrowser/23.5.4.674 Yowser/2.5 Safari/537.36");

            string url = "https://bus.gov.ru/public-rest/api/epbs/fap?lastUpdateFrom=04.12.2019&lastUpdateTo=10.12.2019&page=0&size=100";

            // Act
            HttpResponseMessage httpResponse = await httpClient.GetAsync(url);

            // Assert
            Assert.IsNotNull(httpResponse);
            Assert.AreEqual(HttpStatusCode.OK, httpResponse.StatusCode);
        }
    }
}

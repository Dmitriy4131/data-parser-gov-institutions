using System;
using Npgsql;
using System.Data;
using NUnit.Framework;
using Assert = NUnit.Framework.Assert;

namespace DataParserGovInstitutions.Tests
{
    public class DataDownloaderTests
    {
        [Test]
        public void GetConnection_ValidConnectionString_ConnectionEstablished()
        {
            // Arrange
            DataProcessor dataProcessor = new DataProcessor();

            // Act
            NpgsqlConnection connection = null;
            Exception exception = null;
            try
            {
                connection = dataProcessor.GetConnection();
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            Assert.IsNotNull(connection, "Connection was not established.");
            Assert.IsNull(exception, $"An exception occurred while establishing the connection: {exception?.Message}");
            Assert.AreEqual(ConnectionState.Open, connection.State, "Connection state is not open.");
        }
    }
}
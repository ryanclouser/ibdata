using System;
using System.IO;
using System.Collections.Generic;
using System.Globalization;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using IB;
using IBApi;
using IB.messages;

namespace IBData
{
    class Program
    {
        class Settings
        {
            public string username = "root";
            public string password = string.Empty;
            public string server = "127.0.0.1";
            public uint port = 3306;

            public string TWS = "127.0.0.1";
            public int TWSPort = 7497;
            public int ClientID = 10;
        }

        static DateTime IBtoDateTime(string date)
        {
            if (date.Length == 8)
            {
                // Daily bars
                return DateTime.ParseExact(date, "yyyyMMdd", CultureInfo.InvariantCulture);
            }

            return DateTime.ParseExact(date, "yyyyMMdd  HH:mm:ss", CultureInfo.InvariantCulture);
        }

        static DateTime UnixTimeStampToDateTime(long unixTimeStamp)
        {
            var dt = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            return TimeZoneInfo.ConvertTimeFromUtc(dt.AddSeconds(unixTimeStamp), eastern());
        }

        static TimeZoneInfo eastern()
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            }
            catch (Exception)
            {
                // macOS / Linux
                return TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
            }
        }

        static string FuturesExchange(string symbol)
        {
            symbol = symbol.Replace("@", "");

            switch (symbol)
            {
                case "ES":
                case "MES":
                    return "GLOBEX";
                case "CL":
                case "GC":
                case "HG":
                case "HO":
                case "PA":
                case "PL":
                case "QG":
                case "QM":
                case "SI":
                    return "NYMEX";
                case "ZN":
                case "KE":
                case "TN":
                case "UB":
                case "YM":
                case "ZB":
                case "ZC":
                case "ZF":
                case "ZL":
                case "ZM":
                case "ZO":
                case "ZQ":
                case "ZS":
                case "ZT":
                case "ZW":
                    return "ECBOT";
                case "CC":
                case "OJ":
                case "CT":
                case "KC":
                case "SB":
                    return "NYBOT";
                case "YG":
                case "YI":
                    return "NYSELIFFE";
            }

            return string.Empty;
        }

        static void Create(MySqlConnection db)
        {
            using (var cmd = new MySqlCommand(@"CREATE DATABASE IF NOT EXISTS IB CHARSET UTF8", db))
                cmd.ExecuteNonQuery();

            using (var cmd = new MySqlCommand(@"USE IB", db))
                cmd.ExecuteNonQuery();

            using (var cmd = new MySqlCommand(@"CREATE TABLE IF NOT EXISTS symbols (id INTEGER PRIMARY KEY AUTO_INCREMENT, symbol VARCHAR(16), UNIQUE INDEX `idx_symbol` (`symbol`))", db))
                cmd.ExecuteNonQuery();

            using (var cmd = new MySqlCommand(@"CREATE TABLE IF NOT EXISTS candles (id INTEGER PRIMARY KEY AUTO_INCREMENT, symbol INTEGER, date DATETIME, granularity INTEGER, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, trades INTEGER, FOREIGN KEY (symbol) REFERENCES symbols(id) ON DELETE CASCADE)", db))
                cmd.ExecuteNonQuery();

            using (var cmd = new MySqlCommand(@"CREATE TABLE IF NOT EXISTS ticks (id INTEGER PRIMARY KEY AUTO_INCREMENT, symbol INTEGER, date DATETIME, price DOUBLE, size INTEGER, FOREIGN KEY (symbol) REFERENCES symbols(id) ON DELETE CASCADE)", db))
                cmd.ExecuteNonQuery();

            try
            {
                using (var cmd = new MySqlCommand(@"CREATE UNIQUE INDEX idx_candles ON candles (symbol, granularity, date)", db))
                    cmd.ExecuteNonQuery();
            }
            catch (Exception) { }

            try
            {
                using (var cmd = new MySqlCommand(@"CREATE INDEX idx_ticks ON ticks (symbol, date, price, size)", db))
                    cmd.ExecuteNonQuery();
            }
            catch (Exception) { }
        }

        static void Insert(MySqlConnection db, MySqlTransaction tr, string symbol, int granularity, HistoricalDataMessage msg)
        {
            bool inserted = false;

            using (var cmd = new MySqlCommand(@"INSERT IGNORE INTO candles(symbol, date, granularity, open, high, low, close, volume, trades) VALUES((SELECT id FROM symbols WHERE symbol=@symbol LIMIT 1), @date, @granularity, @open, @high, @low, @close, @volume, @trades)", db, tr))
            {
                cmd.Parameters.Add(new MySqlParameter("@symbol", symbol));
                cmd.Parameters.Add(new MySqlParameter("@date", IBtoDateTime(msg.Date)));
                cmd.Parameters.Add(new MySqlParameter("@granularity", granularity));
                cmd.Parameters.Add(new MySqlParameter("@open", msg.Open));
                cmd.Parameters.Add(new MySqlParameter("@high", msg.High));
                cmd.Parameters.Add(new MySqlParameter("@low", msg.Low));
                cmd.Parameters.Add(new MySqlParameter("@close", msg.Close));
                cmd.Parameters.Add(new MySqlParameter("@volume", msg.Volume));
                cmd.Parameters.Add(new MySqlParameter("@trades", msg.Count));
                inserted = cmd.ExecuteNonQuery() > 0;
            }

            if (!inserted)
            {
                using (var cmd = new MySqlCommand(@"UPDATE candles SET open=@open, high=@high, low=@low, close=@close, volume=@volume, trades=@trades WHERE symbol=(SELECT id FROM symbols WHERE symbol=@symbol LIMIT 1) AND date=@date AND granularity=@granularity LIMIT 1", db, tr))
                {
                    cmd.Parameters.Add(new MySqlParameter("@symbol", symbol));
                    cmd.Parameters.Add(new MySqlParameter("@date", IBtoDateTime(msg.Date)));
                    cmd.Parameters.Add(new MySqlParameter("@granularity", granularity));
                    cmd.Parameters.Add(new MySqlParameter("@open", msg.Open));
                    cmd.Parameters.Add(new MySqlParameter("@high", msg.High));
                    cmd.Parameters.Add(new MySqlParameter("@low", msg.Low));
                    cmd.Parameters.Add(new MySqlParameter("@close", msg.Close));
                    cmd.Parameters.Add(new MySqlParameter("@volume", msg.Volume));
                    cmd.Parameters.Add(new MySqlParameter("@trades", msg.Count));
                }
            }
        }

        static void Insert(MySqlConnection db, MySqlTransaction tr, string symbol, HistoricalTickLast[] ticks)
        {
            foreach (var t in ticks)
            {
                using (var cmd = new MySqlCommand(@"INSERT INTO ticks(symbol, date, price, size) VALUES((SELECT id FROM symbols WHERE symbol=@symbol LIMIT 1), @date, @price, @size)", db, tr))
                {
                    cmd.Parameters.Add(new MySqlParameter("@symbol", symbol));
                    cmd.Parameters.Add(new MySqlParameter("@date", UnixTimeStampToDateTime(t.Time)));
                    cmd.Parameters.Add(new MySqlParameter("@price", t.Price));
                    cmd.Parameters.Add(new MySqlParameter("@size", t.Size));
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static void Insert(MySqlConnection db, string symbol)
        {
            using (var cmd = new MySqlCommand(@"INSERT IGNORE INTO symbols(symbol) VALUES(@symbol)", db))
            {
                cmd.Parameters.Add(new MySqlParameter("@symbol", symbol));
                cmd.ExecuteNonQuery();
            }
        }

        static void Main(string[] args)
        {
            int reqId = 0;
            Settings settings;

            try
            {
                settings = JsonConvert.DeserializeObject<Settings>(File.ReadAllText(@"settings.json"));
            }
            catch (Exception)
            {
                settings = new Settings();
                File.WriteAllText(@"settings.json", JsonConvert.SerializeObject(settings, Formatting.Indented));

                Console.WriteLine("Defaults have been saved to 'settings.json'");
                Console.ReadKey();
                return;
            }

            var cnx = new MySqlConnectionStringBuilder();
            cnx.UserID = settings.username;
            cnx.Password = settings.password;
            cnx.Server = settings.server;
            cnx.Port = settings.port;

            MySqlTransaction tr = null;
            var mysql = new MySqlConnection(cnx.ToString());
            mysql.Open();

            // Create MySQL database and tables
            Create(mysql);

            Console.Write("Symbol: ");
            string symbol = Console.ReadLine();

            Console.Write("Duration (days): ");
            int days = Convert.ToInt32(Console.ReadLine());

            Console.Write("Retrieve ticks [y/n]: ");
            bool retrieveTicks = (Console.ReadLine() == "y");
            Console.WriteLine();

            DateTime end = DateTime.Now;
            DateTime start = end.AddDays(-days);

            var signal = new EReaderMonitorSignal();
            var client = new IBClient(signal);
            client.ClientSocket.eConnect(settings.TWS, settings.TWSPort, settings.ClientID);

            var c = new Contract();
            c.Currency = "USD";

            if (symbol.StartsWith("@"))
            {
                c.Symbol = symbol.Replace("@", "");
                c.SecType = "CONTFUT";
                c.Exchange = FuturesExchange(symbol);                
            }
            else
            {
                c.Symbol = symbol;
                c.Exchange = "SMART";
                c.SecType = "STK";
                c.PrimaryExch = "ISLAND";   // fixes ambiguity
            }

            var contracts = new List<ContractDetails>();

            client.ContractDetails += (ContractDetailsMessage msg) =>
            {
                contracts.Add(msg.ContractDetails);
            };

            client.ContractDetailsEnd += (ContractDetailsEndMessage) =>
            {
                if (contracts.Count == 0)
                {
                    Console.WriteLine("No contracts found for {0}", symbol);
                }
                else
                {
                    ContractDetails closest = null;

                    foreach (var cd in contracts)
                    {
                        // No date (not futures)
                        if (string.IsNullOrEmpty(cd.Contract.LastTradeDateOrContractMonth))
                        {
                            closest = cd;
                            break;
                        }

                        var dt1 = DateTime.ParseExact(cd.Contract.LastTradeDateOrContractMonth, "yyyyMMdd", CultureInfo.InvariantCulture);

                        // Skip the contract if there's only 5 days left
                        if ((dt1 - DateTime.Now).Days <= 5)
                            continue;

                        // Find the closest contract
                        if (closest == null || dt1 < DateTime.ParseExact(closest.Contract.LastTradeDateOrContractMonth, "yyyyMMdd", CultureInfo.InvariantCulture))
                        {
                            closest = cd;
                        }
                    }

                    if (closest == null)
                    {
                        Console.WriteLine("Contract not found for {0}", symbol);
                    }
                    else
                    {
                        // Store the symbol
                        Insert(mysql, symbol);

                        // Transaction for bulk inserting
                        tr = mysql.BeginTransaction();

                        c = closest.Contract;

                        // Request candles
                        client.ClientSocket.reqHistoricalData(reqId++, closest.Contract, end.ToString("yyyyMMdd HH:mm:ss"), string.Format("{0} D", days), "1 min", "TRADES", 0, 1, false, null);
                    }
                }
            };

            // Request contract data
            client.ClientSocket.reqContractDetails(reqId++, c);

            client.HistoricalData += (HistoricalDataMessage msg) =>
            {
                Insert(mysql, tr, symbol, 60, msg);
            };

            client.HistoricalDataEnd += (HistoricalDataEndMessage msg) =>
            {
                // Commit data
                tr.Commit();

                Console.WriteLine("Finished retrieving candles");

                if (retrieveTicks)
                {
                    // New transaction for ticks
                    tr = mysql.BeginTransaction();

                    // Request ticks
                    client.ClientSocket.reqHistoricalTicks(reqId++, c, null, end.ToString("yyyyMMdd HH:mm:ss"), 1000, "TRADES", 0, true, null);
                }
            };

            client.historicalTickLast += (int req, HistoricalTickLast[] ticks, bool done) =>
            {
                Insert(mysql, tr, symbol, ticks);

                if (done)
                {
                    end = UnixTimeStampToDateTime(ticks[0].Time);

                    if (end <= start)
                    {
                        // Commit data
                        tr.Commit();

                        Console.WriteLine("Finished retrieving ticks");
                    }
                    else
                    {
                        client.ClientSocket.reqHistoricalTicks(reqId++, c, null, end.ToString("yyyyMMdd HH:mm:ss"), 1000, "TRADES", 0, true, null);
                    }
                }
            };

            client.Error += (int id, int errorCode, string str, Exception ex) =>
            {
                if (id != -1)
                    Console.WriteLine("{0} {1} {2} {3}", DateTime.Now, id, errorCode, str);
            };

            var reader = new EReader(client.ClientSocket, signal);
            reader.Start();

            while (true)
            {
                signal.waitForSignal();
                reader.processMsgs();
            }
        }
    }
}
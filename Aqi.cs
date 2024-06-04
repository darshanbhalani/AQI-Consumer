using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System.Data.Common;

namespace AQI_Consumer
{
    internal class Aqi
    {
        private static Configurations _configurations;
        private static List<AqiModel> dataList = new List<AqiModel>();
        private static List<List<IncidentModel>> allIncidents = new List<List<IncidentModel>>();
        private static List<IncidentModel> aqiIncidents = new List<IncidentModel>();
        private static NpgsqlConnection connection;
        internal static DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime = 10;
        int thresholdTemperature;

        internal async Task start(ConsumerConfig _config, IConfiguration _configuration, NpgsqlConnection _connection) {
            Console.WriteLine("AQI Consumer Started...");
            connection = _connection;
            Console.WriteLine("Configuration Checking...");
            checkConfiguration();
            Console.WriteLine("Configuration Fetched Successfully...");
            Console.WriteLine("Data Consuming Started...");
            await dataConsumer(_config, _configuration);
        }

        private async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration)
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe(_configuration["BootstrapService:Topic"]);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            var msg = JsonConvert.DeserializeObject<List<AqiModel>>(cr.Value.ToString());
                            dataList.AddRange(msg!);
                            await incidentChecker();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }

        private async Task incidentChecker()
        {
            if ((DateTime.Now - lastExecutionTime).TotalSeconds >= thresholdTime)
            {
                var groupedData = dataList.GroupBy(d => d.PollNumber);

                Console.Clear();
                double averageSpeed = dataList.Average(d => d.Temperature);
                List<IncidentModel> incidents = new List<IncidentModel>();
                foreach (var group in groupedData)
                {
                    incidents.Add(new IncidentModel
                    {
                        PollNumber = group.Key,

                        AQI = calculateOverallAQI(_configurations, group.Average(d => d.PM10), group.Average(d => d.PM2_5), group.Average(d => d.NO2), group.Average(d => d.O3), group.Average(d => d.CO), group.Average(d => d.SO2), group.Average(d => d.PM10), group.Average(d => d.PB)),

                        Min_PM10 = group.Min(d => d.PM10),
                        Max_PM10 = group.Max(d => d.PM10),
                        Avg_PM10 = group.Average(d => d.PM10),

                        Min_PM2_5 = group.Min(d => d.PM2_5),
                        Max_PM2_5 = group.Max(d => d.PM2_5),
                        Avg_PM2_5 = group.Average(d => d.PM2_5),

                        Min_NO2 = group.Min(d => d.NO2),
                        Max_NO2 = group.Max(d => d.NO2),
                        Avg_NO2 = group.Average(d => d.NO2),

                        Min_O3 = group.Min(d => d.O3),
                        Max_O3 = group.Max(d => d.O3),
                        Avg_O3 = group.Average(d => d.O3),

                        Min_CO = group.Min(d => d.CO),
                        Max_CO = group.Max(d => d.CO),
                        Avg_CO = group.Average(d => d.CO),

                        Min_SO2 = group.Min(d => d.SO2),
                        Max_SO2 = group.Max(d => d.SO2),
                        Avg_SO2 = group.Average(d => d.SO2),

                        Min_NH3 = group.Min(d => d.NH3),
                        Max_NH3 = group.Max(d => d.NH3),
                        Avg_NH3 = group.Average(d => d.NH3),

                        Min_PB = group.Min(d => d.PB),
                        Max_PB = group.Max(d => d.PB),
                        Avg_PB = group.Average(d => d.PB),

                        Min_Temperature = group.Min(d => d.Temperature),
                        Max_Temperature = group.Max(d => d.Temperature),
                        Avg_Temperature = group.Average(d => d.Temperature),

                        Min_Wind = group.Min(d => d.Wind),
                        Max_Wind = group.Max(d => d.Wind),
                        Avg_Wind = group.Average(d => d.Wind),

                        Min_Pressure = group.Min(d => d.Pressure),
                        Max_Pressure = group.Max(d => d.Pressure),
                        Avg_Pressure = group.Average(d => d.Pressure),

                        Min_Precip = group.Min(d => d.Precip),
                        Max_Precip = group.Max(d => d.Precip),
                        Avg_Precip = group.Average(d => d.Precip),

                        Min_Visibility = group.Min(d => d.Visibility),
                        Max_Visibility = group.Max(d => d.Visibility),
                        Avg_Visibility = group.Average(d => d.Visibility),

                        Min_Humidity = group.Min(d => d.Humidity),
                        Max_Humidity = group.Max(d => d.Humidity),
                        Avg_Humidity = group.Average(d => d.Humidity),

                        Min_Uv = group.Min(d => d.Uv),
                        Max_Uv = group.Max(d => d.Uv),
                        Avg_Uv = group.Average(d => d.Uv),

                        Min_Gust = group.Min(d => d.Gust),
                        Max_Gust = group.Max(d => d.Gust),
                        Avg_Gust = group.Average(d => d.Gust),

                        Min_Feelslike = group.Min(d => d.Feelslike),
                        Max_Feelslike = group.Max(d => d.Feelslike),
                        Avg_Feelslike = group.Average(d => d.Feelslike),

                        Area = group.Min(d => d.Area),
                        City = group.Min(d => d.City),
                        State = group.Min(d => d.State),

                        StartTime = group.Min(d => DateTime.Parse(d.TimeStamp.ToString())),
                        EndTime = group.Max(d => DateTime.Parse(d.TimeStamp.ToString())),
                    }) ;
                }

                displayData(incidents);

                dataList.Clear();
                allIncidents.Clear();
                await checkConfiguration();
                lastExecutionTime = DateTime.Now;
            }
        }

        //private async Task saveIncidents()
        //{
        //    if (temperatureIncidents.Count > 0 && connection != null)
        //    {
        //        long[] pollNumber = temperatureIncidents.Select(model => model.PollNumber).ToArray();
        //        double[] averageTemperature = temperatureIncidents.Select(model => model.AverageTemperature).ToArray();
        //        string[] description = temperatureIncidents.Select(model => model.Description).ToArray();
        //        string[] area = temperatureIncidents.Select(model => model.Area).ToArray();
        //        DateTime[] starttime = temperatureIncidents.Select(model => model.StartTime).ToArray();
        //        DateTime[] endtime = temperatureIncidents.Select(model => model.EndTime).ToArray();
        //        temperatureIncidents.Clear();

        //        using (NpgsqlCommand cmd = new NpgsqlCommand($"select addtemperatureincidents(@in_pollnumber,@in_area,@in_description,@in_threshold,@in_interval,@in_starttime,@in_endtime);", connection))
        //        {
        //            cmd.Parameters.Add(new NpgsqlParameter("in_pollnumber", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = pollNumber.ToArray() });
        //            cmd.Parameters.Add(new NpgsqlParameter("in_area", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = area.ToArray() });
        //            cmd.Parameters.Add(new NpgsqlParameter("in_threshold", thresholdTemperature));
        //            cmd.Parameters.Add(new NpgsqlParameter("in_interval", thresholdTemperature));
        //            cmd.Parameters.Add(new NpgsqlParameter("in_description", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = description.ToArray() });
        //            cmd.Parameters.Add(new NpgsqlParameter("in_starttime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = starttime.ToArray() });
        //            cmd.Parameters.Add(new NpgsqlParameter("in_endtime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = endtime.ToArray() });
        //            cmd.ExecuteNonQuery();
        //        }
        //    }
        //}

        private async Task checkConfiguration()
        {
            if (_configurations == null)
            {
                _configurations = new Configurations();
            }
            using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from getaqiconfigurations()", connection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        _configurations.Categories.Add(reader.GetString(1));
                        _configurations.PM10Breakpoints.Add(reader.GetDouble(2));
                        _configurations.PM10Breakpoints.Add(reader.GetDouble(3));
                        _configurations.PM25Breakpoints.Add(reader.GetDouble(4));
                        _configurations.PM25Breakpoints.Add(reader.GetDouble(5));
                        _configurations.NO2Breakpoints.Add(reader.GetDouble(6));
                        _configurations.NO2Breakpoints.Add(reader.GetDouble(7));
                        _configurations.O3Breakpoints.Add(reader.GetDouble(8));
                        _configurations.O3Breakpoints.Add(reader.GetDouble(9));
                        _configurations.COBreakpoints.Add(reader.GetDouble(10));
                        _configurations.COBreakpoints.Add(reader.GetDouble(11));
                        _configurations.SO2Breakpoints.Add(reader.GetDouble(12));
                        _configurations.SO2Breakpoints.Add(reader.GetDouble(13));
                        _configurations.NH3Breakpoints.Add(reader.GetDouble(14));
                        _configurations.NH3Breakpoints.Add(reader.GetDouble(15));
                        _configurations.PBBreakpoints.Add(reader.GetDouble(16));
                        _configurations.PBBreakpoints.Add(reader.GetDouble(17));
                    }
                }
            }

            using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from getaqicategoryrange()", connection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        _configurations.CategoriesRange.Add(reader.GetDouble(1));
                    }
                }
            }
        }

        private int calculateAQI(double concentration, List<double> breakpoints)
        {
            double AQI = 0;
            for (int i = 0; i < breakpoints.Count - 1; i += 2)
            {
                if (concentration >= breakpoints[i] && concentration <= breakpoints[i + 1])
                {
                    AQI = (_configurations.CategoriesRange[i / 2 + 1] - _configurations.CategoriesRange[i / 2]) / (breakpoints[i + 1] - breakpoints[i]) * (concentration - breakpoints[i]) + _configurations.CategoriesRange[i / 2];
                    break;
                }
            }
            return (int)Math.Round(AQI);
        }

        private int calculateOverallAQI(Configurations _configuration, double _PM10, double _PM25, double _NO2, double _O3, double _CO, double _SO2, double _NH3, double _PB)
        {
            int aqiPM10 = calculateAQI(_PM10, _configuration.PM10Breakpoints);
            int aqiPM25 = calculateAQI(_PM25, _configuration.PM25Breakpoints);
            int aqiNO2 = calculateAQI(_NO2, _configuration.NO2Breakpoints);
            int aqiO3 = calculateAQI(_O3, _configuration.O3Breakpoints);
            int aqiCO = calculateAQI(_CO, _configuration.COBreakpoints);
            int aqiSO2 = calculateAQI(_SO2, _configuration.SO2Breakpoints);
            int aqiNH3 = calculateAQI(_NH3, _configuration.NH3Breakpoints);
            int aqiPB = calculateAQI(_PB, _configuration.PBBreakpoints);

            int overallAQI = Math.Max(aqiPM10, Math.Max(aqiPM25, Math.Max(aqiNO2, Math.Max(aqiO3, Math.Max(aqiCO, Math.Max(aqiSO2, Math.Max(aqiNH3, aqiPB)))))));

            return overallAQI;
        }

        private void displayData(List<IncidentModel> _incidents)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine($"| {"Poll Number",-15} | {"AQI",-15} | {"Category",-20} | {"Area",-20} | {"City",-20} | {"State",-20}| {"Start Time",-20} | {"End Time",-20} |");
            Console.WriteLine("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
            Console.ResetColor();
            foreach (var i in _incidents)
            {
                Console.Write($"| {i.PollNumber,-15} | ");
                Console.ForegroundColor = ConsoleColor.Black;
                if (_configurations.CategoriesRange[1] > i.AQI && i.AQI >= _configurations.CategoriesRange[0])
                {
                    Console.BackgroundColor = ConsoleColor.DarkGreen;
                }
                 if (_configurations.CategoriesRange[2] > i.AQI && i.AQI >= _configurations.CategoriesRange[1])
                {
                    Console.BackgroundColor = ConsoleColor.Yellow;
                }
                 if (_configurations.CategoriesRange[3] > i.AQI && i.AQI >= _configurations.CategoriesRange[2])
                {
                    Console.BackgroundColor = ConsoleColor.DarkYellow;
                }
                 if (_configurations.CategoriesRange[4] > i.AQI && i.AQI >= _configurations.CategoriesRange[3])
                {
                    Console.BackgroundColor = ConsoleColor.Red;
                }
                 if (_configurations.CategoriesRange[5] > i.AQI && i.AQI >= _configurations.CategoriesRange[4])
                {
                    Console.BackgroundColor = ConsoleColor.DarkRed;
                }
                if (_configurations.CategoriesRange[5] <= i.AQI)
                {
                    Console.BackgroundColor = ConsoleColor.DarkMagenta;
                }
                Console.Write($"{i.AQI,-15} ");
                Console.ResetColor();

                Console.WriteLine($"| {category(i.AQI),-20} | {i.Area,-20} | {i.City,-20} | {i.State,-19} | {i.StartTime,-20} | {i.EndTime,-20} |");
            }
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
            Console.ResetColor();
            Console.Write("Loading...");
        }

        private string category(double aqi)
        {
            if (_configurations.CategoriesRange[1] > aqi && aqi >= _configurations.CategoriesRange[0])
            {
                return _configurations.Categories[0];
            }
            if (_configurations.CategoriesRange[2] > aqi && aqi >= _configurations.CategoriesRange[1])
            {
                return _configurations.Categories[1];
            }
            if (_configurations.CategoriesRange[3] > aqi && aqi >= _configurations.CategoriesRange[2])
            {
                return _configurations.Categories[2];
            }
            if (_configurations.CategoriesRange[4] > aqi && aqi >= _configurations.CategoriesRange[3])
            {
                return _configurations.Categories[3];
            }
            if (_configurations.CategoriesRange[5] > aqi && aqi >= _configurations.CategoriesRange[4])
            {
                return _configurations.Categories[4];
            }
            if (_configurations.CategoriesRange[5] <= aqi)
            {
                return _configurations.Categories[5];
            }
            return "Not Found";
        }
    }
}
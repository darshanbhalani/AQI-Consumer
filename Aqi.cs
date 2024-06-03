using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

namespace AQI_Consumer
{
    internal class Aqi
    {
        private List<AqiModel> dataList = new List<AqiModel>();
        private List<List<IncidentModel>> allIncidents = new List<List<IncidentModel>>();
        private List<IncidentModel> aqiIncidents = new List<IncidentModel>();
        internal DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime = 10;
        int thresholdTemperature = 45;
        private NpgsqlConnection connection;

        internal async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration, NpgsqlConnection _connection)
        {
            connection = _connection;
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

                Double minPM10 = dataList.Min(x => x.PM10);
                Double maxPM10 = dataList.Max(x => x.PM10);



                DateTime startTime = dataList.Min(d => DateTime.Parse(d.TimeStamp.ToString()));
                DateTime endTime = dataList.Max(d => DateTime.Parse(d.TimeStamp.ToString()));


                Console.Clear();
                Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
                Console.WriteLine($"Threshold Speed = {thresholdTemperature} °C");
                double averageSpeed = dataList.Average(d => d.Temperature);
                Console.WriteLine($"Total Temperature Pools = {groupedData.Count()}\n");

                List<IncidentModel> incidents = new List<IncidentModel>();
                foreach (var group in groupedData)
                {
                    IncidentModel incident = new IncidentModel
                    {
                        PollNumber = group.Key,

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

                        StartTime = group.Min(d => DateTime.Parse(d.TimeStamp.ToString())),
                        EndTime = group.Max(d => DateTime.Parse(d.TimeStamp.ToString())),
                    };
                    incidents.Add(incident);
                }

                allIncidents.Add(incidents);


                PrintData(allIncidents[0]);


                dataList.Clear();
                allIncidents.Clear();
                //await checkConfiguration();
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

        //internal async Task checkConfiguration()
        //{
        //    using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from configurations where configurationid=2 and isdeleted=false;", connection))
        //    {
        //        using (var reader = cmd.ExecuteReader())
        //        {
        //            if (reader.Read())
        //            {
        //                thresholdTemperature = reader.GetInt32(2);
        //                thresholdTime = reader.GetInt32(3);
        //            }
        //        }

        //    }
        //}

        public void PrintData(List<IncidentModel> incidents)
        {
            // Define the column headers and their respective widths
            string[] headers = { "PollNumber", "PM10", "PM2.5", "NO2", "O3", "CO", "SO2", "NH3", "PB", "Temperature", "Wind", "Pressure", "Precip", "Visibility", "Humidity", "Uv", "Gust", "Feelslike", "StartTime", "EndTime" };
            int[] widths = { 12, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 20, 20 };

            // Print the top border
            PrintLine(widths);

            // Print the main headers
            PrintRow(headers, widths);
            PrintLine(widths);

            // Print the sub headers
            string[] subHeaders = new string[headers.Length];
            subHeaders[0] = ""; // PollNumber has no sub-headers
            for (int i = 1; i < subHeaders.Length - 2; i++) // -2 to skip StartTime and EndTime
            {
                subHeaders[i] = "Min    Max    Avg";
            }
            subHeaders[subHeaders.Length - 2] = ""; // StartTime has no sub-headers
            subHeaders[subHeaders.Length - 1] = ""; // EndTime has no sub-headers
            PrintRow(subHeaders, widths);
            PrintLine(widths);

            // Print the data rows
            foreach (var incident in incidents)
            {
                PrintRow(new string[]
                {
                    incident.PollNumber.ToString(),
                    $"{incident.Min_PM10,-6}{incident.Max_PM10,-6}{incident.Avg_PM10,-6}",
                    $"{incident.Min_PM2_5,-6}{incident.Max_PM2_5,-6}{incident.Avg_PM2_5,-6}",
                    $"{incident.Min_NO2,-6}{incident.Max_NO2,-6}{incident.Avg_NO2,-6}",
                    $"{incident.Min_O3,-6}{incident.Max_O3,-6}{incident.Avg_O3,-6}",
                    $"{incident.Min_CO,-6}{incident.Max_CO,-6}{incident.Avg_CO,-6}",
                    $"{incident.Min_SO2,-6}{incident.Max_SO2,-6}{incident.Avg_SO2,-6}",
                    $"{incident.Min_NH3,-6}{incident.Max_NH3,-6}{incident.Avg_NH3,-6}",
                    $"{incident.Min_PB,-6}{incident.Max_PB,-6}{incident.Avg_PB,-6}",
                    $"{incident.Min_Temperature,-6}{incident.Max_Temperature,-6}{incident.Avg_Temperature,-6}",
                    $"{incident.Min_Wind,-6}{incident.Max_Wind,-6}{incident.Avg_Wind,-6}",
                    $"{incident.Min_Pressure,-6}{incident.Max_Pressure,-6}{incident.Avg_Pressure,-6}",
                    $"{incident.Min_Precip,-6}{incident.Max_Precip,-6}{incident.Avg_Precip,-6}",
                    $"{incident.Min_Visibility,-6}{incident.Max_Visibility,-6}{incident.Avg_Visibility,-6}",
                    $"{incident.Min_Humidity,-6}{incident.Max_Humidity,-6}{incident.Avg_Humidity,-6}",
                    $"{incident.Min_Uv,-6}{incident.Max_Uv,-6}{incident.Avg_Uv,-6}",
                    $"{incident.Min_Gust,-6}{incident.Max_Gust,-6}{incident.Avg_Gust,-6}",
                    $"{incident.Min_Feelslike,-6}{incident.Max_Feelslike,-6}{incident.Avg_Feelslike,-6}",
                    incident.StartTime.ToString(),
                    incident.EndTime.ToString()
                }, widths);
                PrintLine(widths);
            }
        }

        static void PrintLine(int[] widths)
        {
            Console.WriteLine(new string('-', widths.Sum() + widths.Length + 1));
        }

        static void PrintRow(string[] columns, int[] widths)
        {
            string row = "|";
            for (int i = 0; i < columns.Length; i++)
            {
                row += columns[i].PadRight(widths[i]) + "|";
            }
            Console.WriteLine(row);
        }
    }
}
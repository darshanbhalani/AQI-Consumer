namespace AQI_Consumer
{
    internal class IncidentModel
    {
        public long PollNumber { get; set; }

        public double Min_PM10 { get; set; }
        public double Max_PM10 { get; set; }
        public double Avg_PM10 { get; set; }

        public double Min_PM2_5 { get; set; }
        public double Max_PM2_5 { get; set; }
        public double Avg_PM2_5 { get; set; }

        public double Min_NO2 { get; set; }
        public double Max_NO2 { get; set; }
        public double Avg_NO2 { get; set; }

        public double Min_O3 { get; set; }
        public double Max_O3 { get; set; }
        public double Avg_O3 { get; set; }

        public double Min_CO { get; set; }
        public double Max_CO { get; set; }
        public double Avg_CO { get; set; }

        public double Min_SO2 { get; set; }
        public double Max_SO2 { get; set; }
        public double Avg_SO2 { get; set; }

        public double Min_NH3 { get; set; }
        public double Max_NH3 { get; set; }
        public double Avg_NH3 { get; set; }

        public double Min_PB { get; set; }
        public double Max_PB { get; set; }
        public double Avg_PB { get; set; }


        public double Min_Temperature { get; set; }
        public double Max_Temperature { get; set; }
        public double Avg_Temperature { get; set; }

        public double Min_Wind { get; set; }
        public double Max_Wind { get; set; }
        public double Avg_Wind { get; set; }

        public double Min_Pressure { get; set; }
        public double Max_Pressure { get; set; }
        public double Avg_Pressure { get; set; }

        public double Min_Precip { get; set; }
        public double Max_Precip { get; set; }
        public double Avg_Precip { get; set; }

        public double Min_Visibility { get; set; }
        public double Max_Visibility { get; set; }
        public double Avg_Visibility { get; set; }

        public double Min_Humidity { get; set; }
        public double Max_Humidity { get; set; }
        public double Avg_Humidity { get; set; }

        public double Min_Uv { get; set; }
        public double Max_Uv { get; set; }
        public double Avg_Uv { get; set; }

        public double Min_Gust { get; set; }
        public double Max_Gust { get; set; }
        public double Avg_Gust { get; set; }

        public double Min_Feelslike { get; set; }
        public double Max_Feelslike { get; set; }
        public double Avg_Feelslike { get; set; }


        public DateTime StartTime {  get; set; }
        public DateTime EndTime {  get; set; }
    }
}

namespace AQI_Consumer
{
    internal class AqiModel
    {
        public long PollNumber { get; set; }

        public double PM10 { get; set; }
        public double PM2_5 { get; set; }
        public double NO2 { get; set; }
        public double O3 { get; set; }
        public double CO { get; set; }
        public double SO2 { get; set; }
        public double NH3 { get; set; }
        public double PB { get; set; }

        public double Temperature { get; set; }
        public double Wind { get; set; }
        public double Pressure { get; set; }
        public double Precip { get; set; }
        public double Visibility { get; set; }
        public double Humidity { get; set; }
        public double Uv { get; set; }
        public double Gust { get; set; }
        public double Feelslike { get; set; }

        public string XCoordinate { get; set; }
        public string YCoordinate { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Area { get; set; }

        public DateTime TimeStamp { get; set; }
    }
}

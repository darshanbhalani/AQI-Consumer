using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace AQI_Consumer
{
    internal class Configurations
    {
        public List<string> Categories { get; set; } = new List<string>();
        public List<double> CategoriesRange { get; set; } = new List<double>();
        public List<double> PM10Breakpoints { get; set; } = new List<double>();
        public List<double> PM25Breakpoints {  get; set; } = new List<double>();
        public List<double> NO2Breakpoints { get; set; } = new List<double>();
        public List<double> O3Breakpoints { get; set; } = new List<double>();
        public List<double> COBreakpoints { get; set; } = new List<double>();
        public List<double> SO2Breakpoints { get; set; } = new List<double>();
        public List<double> NH3Breakpoints { get; set; } = new List<double>();
        public List<double> PBBreakpoints { get; set; } = new List<double>();
    }
}

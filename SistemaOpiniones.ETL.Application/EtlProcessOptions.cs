namespace SistemaOpiniones.ETL.Application
{
    public class EtlProcessOptions
    {
        public const string SectionName = "EtlProcess";
        public List<string> SourcesToRun { get; set; } = new();
    }
}

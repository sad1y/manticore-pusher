// See https://aka.ms/new-console-template for more information
// docker run -p 9306:9306 --name manticore --rm -d manticoresearch/manticore:4.0.2

using System;
using System.Threading.Tasks;
using CommandLine;
using ManticorePusher;


static class Program
{
    public class Options
    {
        [Option('w', "worker-count", Required = true, HelpText = "how many table to create")]
        public int WorkerCount { get; set; }

        [Option('i', "iteration-count", Required = true, HelpText = "how many batch insert to perform")]
        public int IterationCount { get; set; }

        [Option('b', "batch-size", Required = true, HelpText = "how many insert statements command will contains")]
        public int BatchSize { get; set; }

        [Option('c', "connection-string", Required = false, Default = "Server=127.0.0.1;Port=9306;SslMode=None;", HelpText = "connection string to manticore")]
        public string? ConnectionString { get; set; }
    }

    public static async Task Main(string[] args)
    {
        await Parser.Default.ParseArguments<Options>(args).WithParsedAsync(async o => await Run(o));
    }

    private static async Task Run(Options options)
    {
        var pusher = new Pusher(options.ConnectionString!, options.WorkerCount, options.IterationCount, options.BatchSize);
        await pusher.Run();
        Console.WriteLine("done!");
    }
}
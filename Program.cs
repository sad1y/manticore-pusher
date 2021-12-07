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
        [Option('w', "workerCount", Required = true, HelpText = "how many table to create")]
        public int WorkerCount { get; set; }

        [Option('i', "iterationCount", Required = true, HelpText = "how many batch insert to perform")]
        public int IterationCount { get; set; }

        [Option('b', "batchSize", Required = true, HelpText = "how many insert statements command will contains")]
        public int BatchSize { get; set; }
    }

    public static async Task Main(string[] args)
    {
        await Parser.Default.ParseArguments<Options>(args).WithParsedAsync(async o => await Run(o));
    }

    private static async Task Run(Options options)
    {
        var pusher = new Pusher(options.WorkerCount, options.IterationCount, options.BatchSize);
        await pusher.Run();
        Console.WriteLine("done!");
    }
}
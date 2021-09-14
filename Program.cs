using System;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;

namespace BackgroundEmailSenderSample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseShutdownTimeout(TimeSpan.FromSeconds(10))
                .UseEnvironment("Development")
                .UseStartup<Startup>()
                .Build();
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Util.Internal;
using NUnit.Framework;

namespace ParallelTests
{
    [TestFixture]
    public class OneLevelDeepParallelTests
    {
        private const int NumIters = 25;

        private static IEnumerable<int> DoWork(int n)
        {
            Console.WriteLine(
                "Task.CurrentId: {0}; Thread.CurrentThread.ManagedThreadId: {1}",
                Task.CurrentId.HasValue ? Convert.ToString(Task.CurrentId.Value) : "N/A",
                Thread.CurrentThread.ManagedThreadId);

            foreach (var x in Enumerable.Range(1, n))
            {
                // This waits for about 0.5 seconds on my machine.
                Thread.SpinWait(2200000);

                yield return x;
            }
        }

        [Test]
        public void Benchmark()
        {
            Utils.TimeIt(() =>
            {
                var ns = Enumerable.Repeat(50, NumIters);
                var flattenedResults = ns.SelectMany(DoWork);
                var sum = flattenedResults.Sum();
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        [Test]
        public void UsingParallelForEach()
        {
            Utils.TimeIt(() =>
            {
                var ns = Enumerable.Repeat(50, NumIters);
                var parallelResults = new List<List<IEnumerable<int>>>();
                var lockobject = new object();
                Parallel.ForEach(
                    Partitioner.Create(ns, EnumerablePartitionerOptions.NoBuffering),
                    () => new List<IEnumerable<int>>(),
                    (n, _, local) =>
                    {
                        local.Add(DoWork(n));
                        return local;
                    },
                    local =>
                    {
                        lock (lockobject) parallelResults.Add(local);
                    });
                var results = Utils.ConcatAll(parallelResults);
                var flattenedResults = Utils.ConcatAll(results);
                var sum = flattenedResults.Sum();
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        [Test]
        public void UsingAsParallel()
        {
            Utils.TimeIt(() =>
            {
                var ns = Enumerable.Repeat(50, NumIters);

                var sum = ns
                    .AsParallel()
                    .AsUnordered()
                    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    .WithDegreeOfParallelism(8)
                    .SelectMany(DoWork)
                    .Sum();

                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        [Test]
        public void UsingAkkaActors()
        {
            Utils.TimeIt(() =>
            {
                var system = ActorSystem.Create("ParallelTest");
                var parallelMaster = system.ActorOf(Props.Create(typeof (ParallelMaster)), "ParallelMaster");
                var result = parallelMaster.Ask(NumIters).Result;
                var sum = (int) result;
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
                system.Shutdown();
            });
        }

        private class ParallelMaster : ReceiveActor
        {
            private readonly List<IList<int>> _results = new List<IList<int>>();
            private int _numIters;
            private IActorRef _requester;

            public ParallelMaster()
            {
                Receive<int>(numIters =>
                {
                    _numIters = numIters;
                    _requester = Sender;
                    var ns = Enumerable.Repeat(50, numIters);
                    ns.ForEach(n =>
                    {
                        var name = Context.GetChildren().Count() + 1;
                        var worker = Context.ActorOf(Props.Create(typeof(Worker)), $"Worker{name}");
                        worker.Tell(n);
                    });
                });

                Receive<IList<int>>(xs =>
                {
                    _results.Add(xs);
                    if (_results.Count == _numIters)
                    {
                        var sum = Utils.ConcatAll(_results).Sum();
                        _requester.Tell(sum);
                        Context.Stop(Self);
                    }
                });
            }
        }

        private class Worker : ReceiveActor
        {
            public Worker()
            {
                Receive<int>(n =>
                {
                    var xs = DoWork(n);
                    Sender.Tell(xs.ToList());
                    Context.Stop(Self);
                });
            }
        }
    }
}

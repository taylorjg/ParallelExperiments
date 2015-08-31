using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        private const int NumIters = 8;

        [Test]
        public void Benchmark()
        {
            TimeIt(() =>
            {
                var ns = Enumerable.Repeat(50, NumIters);
                var flattenedResults = ns.SelectMany(DoWork);
                var sum = flattenedResults.Sum();
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        [Test]
        public void ForEach()
        {
            TimeIt(() =>
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
                var results = ConcatAll(parallelResults);
                var flattenedResults = ConcatAll(results);
                var sum = flattenedResults.Sum();
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        [Test]
        public void AsParallel()
        {
            TimeIt(() =>
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

        private class MasterActor : ReceiveActor
        {
            private readonly List<IList<int>> _results = new List<IList<int>>();
            private int _numIters;
            private IActorRef _requester;

            public MasterActor()
            {
                Receive<int>(numIters =>
                {
                    _results.Clear();
                    _numIters = numIters;
                    _requester = Sender;
                    var ns = Enumerable.Repeat(50, numIters);
                    ns.ForEach(n =>
                    {
                        var worker = Context.ActorOf(Props.Create(typeof(WorkerActor)));
                        worker.Tell(n);
                    });
                });

                Receive<IList<int>>(xs =>
                {
                    _results.Add(xs);
                    if (_results.Count == _numIters)
                    {
                        var sum = ConcatAll(_results).Sum();
                        _requester.Tell(sum);
                        Context.Stop(Self);
                    }
                });
            }
        }

        private class WorkerActor : ReceiveActor
        {
            public WorkerActor()
            {
                Receive<int>(n =>
                {
                    Console.WriteLine($"WorkerActor {Self.Path}");
                    var xs = DoWork(n);
                    Sender.Tell(xs.ToList());
                });
            }
        }

        [Test]
        public void UsingAkkaActors()
        {
            TimeIt(() =>
            {
                var system = ActorSystem.Create("ParallelTest");
                var master = system.ActorOf(Props.Create(typeof (MasterActor)));
                var result = master.Ask(NumIters).Result;
                var sum = (int) result;
                Assert.That(sum, Is.EqualTo(Enumerable.Range(1, 50).Sum() * NumIters));
            });
        }

        private static IEnumerable<int> DoWork(int n)
        {
            Console.WriteLine("Task.CurrentId: {0}; Thread.CurrentThread.ManagedThreadId: {1}", Task.CurrentId, Thread.CurrentThread.ManagedThreadId);

            foreach (var x in Enumerable.Range(1, n))
            {
                // This waits for about 0.5 seconds on my machine.
                Thread.SpinWait(2200000);

                yield return x;
            }
        }

        private static void TimeIt(Action action)
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                action();
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine("stopwatch.Elapsed: {0}", stopwatch.Elapsed);
            }
        }

        private static IEnumerable<T> ConcatAll<T>(IEnumerable<IEnumerable<T>> sources)
        {
            foreach (var source in sources)
                using (var e = source.GetEnumerator())
                    while (e.MoveNext())
                        yield return e.Current;
        }
    }
}

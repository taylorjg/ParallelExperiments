using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ParallelTests
{
    public static class Utils
    {
        public static void TimeIt(Action action)
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

        public static IEnumerable<T> ConcatAll<T>(IEnumerable<IEnumerable<T>> sources)
        {
            foreach (var source in sources)
                using (var e = source.GetEnumerator())
                    while (e.MoveNext())
                        yield return e.Current;
        }
    }
}

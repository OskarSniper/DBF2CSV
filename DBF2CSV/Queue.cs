using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace DBF2CSV
{
    public class NonblockingQueue<T>
    {
        readonly int _Size = 0;
        readonly Queue<T> _Queue = new Queue<T>();
        readonly object _Key = new object();
        bool _Quit = false;

        public int Count { get { return _Queue.Count; } }
        public int maxSize { get { return _Size; } }

        public NonblockingQueue()
        {

        }

        public void Quit()
        {
            try
            {
                lock (_Key)
                {
                    _Quit = true;
                    Monitor.PulseAll(_Key);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("LOGGER: " + e);
            }
        }

        public bool Enqueue(T t)
        {
            try
            {
                lock (_Key)
                {
                    while (!_Quit && _Queue.Count < -1)
                        Monitor.Wait(_Key);

                    if (_Quit)
                        return false;

                    _Queue.Enqueue(t);

                    Monitor.PulseAll(_Key);
                }

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("LOGGER: " + e);
                return false;
            }
        }

        public bool Dequeue(out T t)
        {
            try
            {
                t = default(T);

                lock (_Key)
                {
                    while (!_Quit && _Queue.Count == 0)
                        Monitor.Wait(_Key);

                    if (_Queue.Count == 0)
                        return false;

                    t = _Queue.Dequeue();

                    Monitor.PulseAll(_Key);
                }

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("LOGGER: " + e);

                t = default(T);
                return false;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace DBF2CSV
{
    class Program
    {
        private static NonblockingQueue<QueueObject> WriterQueue = new NonblockingQueue<QueueObject>();
        static void Main(string[] args)
        {
            Console.WriteLine("Copyright (c) 2018 Sebastian Waldbauer");
            FileInfo fiInput = new FileInfo(args[0]);
            FileInfo fiOutput = new FileInfo(args[1]);

            OleDbConnection objCon = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + fiInput.Directory.ToString() + ";Extended Properties=dBASE IV;User ID=;Password=;");
            try
            {
                objCon.Open();
                OleDbDataAdapter dbDataAdapter = new OleDbDataAdapter("SELECT * FROM " + fiInput.Name, objCon);
                DataSet dataSet = new DataSet();
                dbDataAdapter.Fill(dataSet);
                objCon.Close();

                runFileWriterThread();

                for (int i = 0; i < dataSet.Tables.Count; i++)
                {
                    string finalOutputFile = string.Empty;
                    if (dataSet.Tables.Count <= 1)
                    {
                        Console.WriteLine("Converting \"" + dataSet.Tables[i].TableName + "\" to " + fiOutput.Name);
                        finalOutputFile = fiOutput.Directory + "\\" + fiOutput.Name;
                    }
                    else
                    {
                        Console.WriteLine("Converting \"" + dataSet.Tables[i].TableName + "\" to " + fiOutput.Name.Replace(fiOutput.Extension, "") + "_" + dataSet.Tables[i].TableName.ToLower() + ".csv");
                        finalOutputFile = fiOutput.Directory + "\\" + fiOutput.Name.Replace(fiOutput.Extension, "") + "_" + dataSet.Tables[i].TableName.ToLower() + ".csv";
                    }

                    Console.WriteLine("Found " + dataSet.Tables[i].Rows.Count + " rows & " + dataSet.Tables[i].Columns.Count + " columns");
                    for (int ix = 0; ix < dataSet.Tables[i].Rows.Count; ix++)
                    {
                        string str = string.Empty;
                        for (int iy = 0; iy < dataSet.Tables[i].Columns.Count; iy++)
                        {
                            str += dataSet.Tables[i].Rows[ix][iy] + ";";
                        }
                        QueueObject qo = new QueueObject();
                        qo.Path = finalOutputFile;
                        qo.Data = str;
                        WriterQueue.Enqueue(qo);
                    }
                }
            } catch(Exception e)
            {
                Console.WriteLine(e);
            }

            Console.ReadLine();
        }

        private static void runFileWriterThread()
        {
            Thread WritingThread = new Thread(() => {
                Thread.CurrentThread.IsBackground = true;

                while (true)
                {
                    if (WriterQueue.Count > 0)
                    {
                        QueueObject item;
                        WriterQueue.Dequeue(out item);

                        const int BufferSize = 65536;
                        try
                        {
                            Console.Write("\rWait till it's 0... Size " + WriterQueue.Count);
                            StreamWriter writer = new StreamWriter(item.Path, true, Encoding.UTF8, BufferSize);
                            writer.WriteLine(item.Data);
                            writer.Close();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error while writing ThreadID: " + Thread.CurrentThread.ManagedThreadId + "!" + e);
                        }

                        // Appending every 100ms ;-)
                        Thread.Sleep(1);
                    }
                    else
                    {
                        //Console.WriteLine("Log empty! Fetching in 10 seconds again!");
                        Thread.Sleep(1000);
                    }
                }
            });
            WritingThread.Start();
        }
    }
}

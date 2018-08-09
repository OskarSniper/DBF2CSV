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
                    using (System.IO.StreamWriter file = new System.IO.StreamWriter(finalOutputFile))
                    {
                        Console.WriteLine("Writing to file [" + finalOutputFile + "]");
                        // Read header!
                        string header = string.Empty;
                        for (int ih = 0; ih < dataSet.Tables[i].Columns.Count; ih++)
                        {
                            header += dataSet.Tables[i].Columns[ih].Caption + ";";
                        }
                        file.WriteLine(header);

                        for (int ix = 0; ix < dataSet.Tables[i].Rows.Count; ix++)
                        {
                            string str = string.Empty;
                            for (int iy = 0; iy < dataSet.Tables[i].Columns.Count; iy++)
                            {
                                str += dataSet.Tables[i].Rows[ix][iy] + ";";
                            }
                            file.WriteLine(str);
                        }

                        Console.WriteLine("Finished writing to [" + finalOutputFile + "]");
                    }
                }
                Console.WriteLine("Finished!");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}

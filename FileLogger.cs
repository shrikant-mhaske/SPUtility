using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
namespace RawData
{
    public class FileLogger
    {
        static string AppPath = string.Empty;


        public static void AppendLog(string filename, string modname, string logtext)
        {
            StreamWriter logfilewriter;
            AppPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase).ToString().Substring(6).Replace(@"\", @"\\");

            try
            {
                logfilewriter = new StreamWriter(AppPath + "\\Logs\\" + filename + ".log", true, Encoding.UTF8);
                if (filename == "Access")
                {
                    logfilewriter.WriteLine(DateTime.Now.ToString()
                        + " Access : "
                        + "AllCustomerRowData "
                        + modname
                        + logtext);
                    logfilewriter.Close();

                }
                else if (filename == "Error")
                {
                    logfilewriter.WriteLine(DateTime.Now.ToString()
                        + " Error:"
                        + "AllCustomerRowData"
                        + modname

                        + logtext);
                    logfilewriter.Close();

                }

                else
                {
                    logfilewriter.WriteLine(DateTime.Now.ToString()
                        + filename
                        + "AllCustomerRowData"
                        + modname

                        + logtext);
                    logfilewriter.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
            }
        }
    }
}

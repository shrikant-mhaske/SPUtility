using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data;
namespace RawData
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            RawDataBusinessLogic objRawDataBusinessLogic = new RawDataBusinessLogic();
            //Comment
            //get cust list
            DataTable custdt = objRawDataBusinessLogic.getCusomerlist();
            //get query list
            DataTable querydt = objRawDataBusinessLogic.getQuerylist();
            //get data one by one masterID
            objRawDataBusinessLogic.getDataOneByOne(custdt, querydt);
        }
    }
}

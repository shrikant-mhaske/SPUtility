using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;
using System.Data;
namespace RawData
{
    static class   DBmain
    {

        static private string sqlConnectionString = "server=SikkaSQLServer;database=SCD;uid=LicenseUser;password=ibrainUNIFY414001;Pooling=false;";


        //get data from SQL server
        static public DataTable getDataFromSQLServer(string query)
        {
            DataTable data = new DataTable();
            try
            {
                SqlConnection con = new SqlConnection();
                con.ConnectionString = sqlConnectionString;
                SqlDataAdapter sda = new SqlDataAdapter(query, sqlConnectionString);
                sda.Fill(data);
                con.Close();
            }
            catch (Exception Ex)
            {
             //   FileLogger.AppendLog("Error", "DBmain=>getDataFromSQLServer", Ex.Message);
            }
            return data;

        }


        //insert data into sqlserver
        static public bool insertDataIntoSQLServer(string query)
        {
            bool ret = false;
            try
            {
                SqlConnection con = new SqlConnection();
                con.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;
                cmd.CommandText = query;
                int cont = cmd.ExecuteNonQuery();
                ret = true;
                con.Close();
            }
            catch (Exception Ex)
            {
              //  FileLogger.AppendLog("Error", "DBmain=>insertDataIntoSql", Ex.Message);
            }
            return ret;
        }


        //get data from  Mysql for particular masterID
        static public DataTable getDataFromMySQL(string DBString, string query)
        {
            DataTable data = new DataTable();
            // DBString = DBString.Replace("10.67.206.111;", "107.21.218.188;");
            // DBString = "Host=107.21.218.188;Port=3306;Database=d8937;Uid=root;Pwd=ibrainUNIFY414001;default command timeout=3600;";
            DBString = "Host=ec2-54-91-174-208.compute-1.amazonaws.com;Port=3306;Database=d10648;Uid=root;Pwd=ibrainUNIFY414001;default command timeout=3600;";
            //
            try
            {
                MySqlConnection con = new MySqlConnection();
                con.ConnectionString = DBString;
                con.Open();
                MySqlDataAdapter mda = new MySqlDataAdapter(query, con);
                mda.Fill(data);
                con.Close();
            }
            catch (Exception Ex)
            {
              //  FileLogger.AppendLog("Error", "DBmain=>getDataFromMySQL", Ex.Message);
            }
            return data;
        }


        //inser list of data in to sql server Db      
        static public bool setSQLServerQueryListInDb(List<string> _querylst, string MID, string PID, int CID)
        {
            bool ret = false;
            try
            {
                SqlTransaction tx = null;
                using (SqlConnection con = new SqlConnection(sqlConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = con;
                        con.Open();
                        tx = con.BeginTransaction();
                        cmd.Transaction = tx;
                        foreach (string sql in _querylst)
                        {
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();

                        }
                        ret = true;
                        tx.Commit();
                        con.Close();
                    }
                }
            }
            catch (Exception Ex)
            {
             //   FileLogger.AppendLog("Error" + MID, "DBmain=>setQueryListInDb=>MID=" + MID + ",CID=" + CID + ",PID=" + PID, Ex.Message);
            }
            return ret;

        }


    }
}

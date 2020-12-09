using System;
using System.Configuration;
using System.Data.SqlClient;

namespace CycleRun_NetCore.SqlConn
{
    class DBUtils
    {
        public static SqlConnection GetDBConnection()
        {
            MyParams myParams = new MyParams();

            string c_string = myParams.Value("ConStr");

            return GetDBConnection( c_string );
        }


        public static SqlConnection GetDBConnection( string connString)
        {
            Console.WriteLine("Connection string: " + connString);

            SqlConnection conn = new SqlConnection(connString);

            return conn;
        }
    }

}
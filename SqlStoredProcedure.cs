using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;


public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]

    /**
     * naturalKey can be multple columns, comma-separated
     * primaryKey must be one single integer-column
     **/
    public static void LoadTable (string srcDataBase, string srcSchema, string srcTable,
                                string tgtDataBase, string tgtSchema, string tgtTable, 
                                string naturalKey, string primaryKey)
    {
        naturalKey = naturalKey.Replace(';', ',');
        string[] nKey = naturalKey.Split(',');

        

        string SQL, connectionString;
        string srcTableName, tgtTableName, checksumCols;
        SqlCommand cmd;
        SqlDataReader dataReader;
        int insertedRecords, updatedRecords, deletedRecords;

        if (srcTable.IndexOf('#') == 0) srcTableName = srcTable;
        else srcTableName = srcDataBase + "." + srcSchema + "." + srcTable;

        if (tgtTable.IndexOf('#') == 0) tgtTableName = tgtTable;
        else tgtTableName = tgtDataBase + "." + tgtSchema + "." + tgtTable;

        connectionString = @"Data Source = (local); Integrated Security = true; Initial Catalog = " + tgtDataBase + ";";
        //connectionString = @"context connection = true;";
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            try
            {
                connection.Open();

                //Prepare
                SqlContext.Pipe.Send("Collecting columns for versioning");

                SQL = "SELECT column_name FROM " + srcDataBase + ".INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + srcTable + "'";
                cmd = new SqlCommand(SQL, connection);
                dataReader = cmd.ExecuteReader();
                checksumCols = "";
                while(dataReader.Read())
                    checksumCols += dataReader.GetString(0) + ", ";
                dataReader.Close();
                dataReader.Dispose();
                cmd.Dispose();

                checksumCols = checksumCols.Substring(0, checksumCols.Length - 2);
                string checksumCalc = "HASHBYTES('SHA2_256',CONCAT(" + checksumCols + ",''))";  // Add dummy value to avoid error if only one column

                SqlContext.Pipe.Send("Create temptable");

                // Make temp table
                SQL = "SELECT SYSDATETIME() load_date INTO #load_date;";
                SQL += "SELECT t." + primaryKey + " AS A3eff2Wq_tgt_sk, " +
                            "t.valid_from AS A3eff2Wq_tgt_valid_from, " +
                            "l.load_date AS A3eff2Wq_src_valid_from, " +
                            "t.chksum AS A3eff2Wq_tgt_chksum, " +
                            "CAST('21000101' AS DATETIME2) AS valid_to, " +
                            "1 AS current_fl, " +
                            "l.load_date AS load_dt, " +
                            "system_user AS rec_src, " +
                            "CASE WHEN s." + naturalKey.Replace(",", " IS NULL AND s.") + " IS NULL THEN 1 ELSE 0 END AS deleted_fl, " +
                            "s.* " +
                        "INTO #LoadTableSrc " +
                        "FROM (SELECT *, " + checksumCalc + " AS chksum FROM " + srcTableName + ") AS s " +
                        "FULL OUTER JOIN (SELECT * FROM " + tgtTableName + " WHERE current_fl = 1 AND deleted_fl = 0) AS t ON ";

                for (int i = 0; i < nKey.Length; i++)
                {
                    SQL += "s." + nKey[i] + " = t." + nKey[i] + " AND ";
                }
                SQL = SQL.Substring(0, SQL.Length - 5);
                SQL += ", #load_date AS l;";


                SqlContext.Pipe.Send(SQL);
                cmd = new SqlCommand(SQL, connection);
                cmd.ExecuteNonQuery();
                cmd.Dispose();

                // Move data - Insert
                SqlContext.Pipe.Send("Inserting new records");

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                using (DataTable dataTable = new DataTable())
                {
                    SQL = "SELECT t." + primaryKey + " + ROW_NUMBER() OVER (ORDER BY s." + naturalKey.Replace(",", ", s.") + ") AS " + primaryKey + ", " +
                        "A3eff2Wq_src_valid_from AS valid_from, " +
                        "s.* " +
                    "FROM #LoadTableSrc s, " +
                    "(SELECT ISNULL(MAX(" + primaryKey + "),0) AS " + primaryKey + " " +
                        "FROM " + tgtTableName + ") AS t " +
                    "WHERE A3eff2Wq_tgt_sk IS null " +
                    "ORDER BY s." + naturalKey.Replace(",", ", s.") + ";";
                    //Log(jobName, SQL, connection);
                    using (SqlDataAdapter dataAdaptor = new SqlDataAdapter(SQL, connection)) dataAdaptor.Fill(dataTable);
                    insertedRecords = dataTable.Rows.Count;

                    foreach (DataColumn c in dataTable.Columns)
                        if (c.ColumnName.IndexOf("A3eff2Wq_") == -1)
                        {
                            //Log(jobName, c.ColumnName, connection);
                            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
                        }

                    bulkCopy.DestinationTableName = tgtTableName;
                    bulkCopy.WriteToServer(dataTable);

                    SqlContext.Pipe.Send("Inserted " + insertedRecords.ToString() + " records");
                }

                // Move data - Update 
                SqlContext.Pipe.Send("Updating changed records");

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                using (DataTable dataTable = new DataTable())
                {
                    SQL = "SELECT A3eff2Wq_tgt_sk AS " + primaryKey + ", " +
                        "A3eff2Wq_src_valid_from AS valid_from, " +
                        "s.* " +
                    "FROM #LoadTableSrc s " +
                    "WHERE A3eff2Wq_tgt_sk IS not null " +
                    "AND deleted_fl = 0 " +
                    "AND chksum != A3eff2Wq_tgt_chksum " +
                    "ORDER BY s." + naturalKey.Replace(",", ", s.") + ";";
                    //SqlContext.Pipe.Send(SQL);
                    using (SqlDataAdapter dataAdaptor = new SqlDataAdapter(SQL, connection)) dataAdaptor.Fill(dataTable);

                    updatedRecords = dataTable.Rows.Count;

                    // Insert
                    foreach (DataColumn c in dataTable.Columns)
                        if (c.ColumnName.IndexOf("A3eff2Wq_") == -1)
                        {
                            //Log(jobName, c.ColumnName, connection);
                            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
                        }
                    bulkCopy.DestinationTableName = tgtTableName;
                    bulkCopy.WriteToServer(dataTable);
                }
                // Close previous version
                SQL = "UPDATE " + tgtTableName + " " +
                            "SET current_fl = 0, " +
                            "valid_to = s.A3eff2Wq_src_valid_from " +
                        "FROM #LoadTableSrc s " +
                        "WHERE s.A3eff2Wq_tgt_sk IS not null " +
                            "AND s.deleted_fl = 0 " +
                            "AND s.chksum != A3eff2Wq_tgt_chksum " +
                            "AND " + tgtTable + "." + primaryKey + " = s.A3eff2Wq_tgt_sk " +
                            "AND " + tgtTable + ".valid_from = s.A3eff2Wq_tgt_valid_from;";
                cmd = new SqlCommand(SQL, connection);
                cmd.ExecuteNonQuery();
                cmd.Dispose();

                SqlContext.Pipe.Send("Updated " + updatedRecords.ToString() + " records");


                // Move data - Delete 
                SqlContext.Pipe.Send("Delete removed records");
                
                using (DataTable dataTable = new DataTable())
                {

                    SQL = "SELECT A3eff2Wq_tgt_sk AS " + primaryKey + ", " +
                                "A3eff2Wq_src_valid_from, " +
                                "A3eff2Wq_tgt_valid_from " +
                            "FROM #LoadTableSrc s " +
                            "WHERE deleted_fl = 1 " +
                            "ORDER BY s." + naturalKey.Replace(",", ", s.") + ";";
                    using (SqlDataAdapter dataAdaptor = new SqlDataAdapter(SQL, connection)) dataAdaptor.Fill(dataTable);

                    deletedRecords = dataTable.Rows.Count;

                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        // Insert and Close
                        SQL = "SELECT * INTO #deleted " +
                                "FROM  " + tgtTableName + " " +
                                "WHERE " + primaryKey + " = " + dataRow[primaryKey].ToString() + " " +
                                "AND current_fl = 1;";
                        SQL += "UPDATE " + tgtTableName + " " +
                                "SET current_fl = 0, " +
                                "valid_to = (SELECT load_date FROM #load_date) " +
                                "WHERE " + primaryKey + " = " + dataRow[primaryKey].ToString() + " " +
                                "AND current_fl = 1;";
                        SQL += "UPDATE #deleted SET deleted_fl = 1, " +
                                    "valid_from = (SELECT load_date FROM #load_date), " +
                                    "rec_src = system_user;";
                        SQL += "INSERT INTO " + tgtTableName + " SELECT * FROM #deleted;";
                        SQL += "DROP TABLE #deleted;";
                        cmd = new SqlCommand(SQL, connection);
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }
                }
                SqlContext.Pipe.Send("Deleted " + deletedRecords.ToString() + " records");

                // Cleanup
                SqlContext.Pipe.Send("Remove temptable");

                SQL = "DROP TABLE #LoadTableSrc;";
                SQL += "DROP TABLE #load_date;";
                cmd = new SqlCommand(SQL, connection);
                cmd.ExecuteNonQuery();
                cmd.Dispose();

                connection.Close();

            }
            catch (Exception)
            {
                SqlContext.Pipe.Send("An error occurred");
                SqlContext.Pipe.Send("NOTE: Parameters to this stored procedure are case sensetive!");
                try
                {
                    cmd = new SqlCommand("DROP TABLE #LoadTableSrc", connection);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
                catch { }
                try
                {
                    cmd = new SqlCommand("DROP TABLE #load_date", connection);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
                catch { }
                try
                {
                    cmd = new SqlCommand("DROP TABLE #deleted", connection);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
                catch { }
                throw;
            }

        }
    }


}

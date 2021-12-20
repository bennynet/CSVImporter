using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Globalization;
using CsvHelper;
using System.Timers;

namespace CSVImporter
{
    public class ImporterMananger
    {

        Timer timer;

        public ImporterMananger() {
            timer = new Timer();
            timer.Interval = 1000;
            timer.Elapsed += Timer_Elapsed;
        }

        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (this.OnMessageOut != null)
            {
                OnMessageOut(this, new Tuple<int, string>(0, string.Format("探索文件中.发现{0}条记录..", total)));
            }
        }

        public async Task<string> Ping(string dbconenctstr)
        {
            
            try
            {
                using (SqlConnection con = new SqlConnection(dbconenctstr))
                {
                    await con.OpenAsync();
                    return "";
                }
            }
            catch (Exception ex)
            {
                return ex.Message;
            }

        }

        private async Task AutoCreatedTable(string tablename, SqlConnection con, string createsql)
        {
            string sql = string.Format("SELECT COUNT(*) FROM sys.objects WHERE name='{0}'", tablename);
            SqlCommand cmd = new SqlCommand(sql, con);
            object o = await cmd.ExecuteScalarAsync();
            if (int.Parse(o.ToString()) == 0)
            {
                SqlCommand cmd2 = new SqlCommand(createsql, con);
                await cmd2.ExecuteNonQueryAsync();
            }
        }

        public event EventHandler<Tuple<int,string>> OnMessageOut; // 0- 文本输出，1- 总量更新，2-导入进度更新

        int total;

        public async Task<string> ImportAsync(string dbconnectstr, string csvfile,string tablename,int batchsize=10000)
        {
            try
            {
                //统计总行数，生成建表脚本
                if (this.OnMessageOut != null)
                {
                    OnMessageOut(this, new Tuple<int, string>(0, "探索文件中..."));
                }
                total = 0;
                Dictionary<string, int> colsdef = new Dictionary<string, int>();

                await Task.Run(()=> {
                   
                    using (var reader = new StreamReader(csvfile))
                    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                    {
                        csv.Read();
                        csv.ReadHeader();
                        var cols = csv.GetRecord<dynamic>();

                        foreach (var item in cols)
                        {
                            colsdef.Add(item.Key, 0);
                        }
                        timer.Start();
                        while (csv.Read())
                        {
                            var rows = csv.GetRecord<dynamic>();
                            foreach (var item in rows)
                            {
                                int len = item.Value.Length;
                                string k = item.Key;
                                if (len > colsdef[k])
                                {
                                    colsdef[k] = len;
                                }
                            }
                            total++;

                        }

                        timer.Stop();

                    }
                });


                if (this.OnMessageOut != null)
                {
                    OnMessageOut(this, new Tuple<int, string>(1, total.ToString()));
                }




                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("CREATE TABLE [dbo].[{0}](", tablename);
                var list = colsdef.ToList();
                for (int i = 0; i < list.Count; i++) {
                    sb.AppendFormat("[{0}] [VARCHAR]({1}) NULL", list[i].Key,list[i].Value>0 ? list[i].Value*2 : 50);
                    if (i < list.Count - 1) {
                        sb.Append(",");
                    }
                }
                sb.Append(")");

                using (var reader = new StreamReader(csvfile))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    using (SqlConnection con = new SqlConnection(dbconnectstr)) {
                        con.Open();
                        await this.AutoCreatedTable(tablename, con, sb.ToString());

                        using (CsvDataReader dr = new CsvDataReader(csv))
                        using (SqlBulkCopy blp = new SqlBulkCopy(con))
                        {
                            blp.NotifyAfter = batchsize;
                            blp.SqlRowsCopied += Blp_SqlRowsCopied;
                            blp.DestinationTableName = tablename;
                            blp.BatchSize = batchsize;

                            await blp.WriteToServerAsync(dr);
                        }

                    }
                }

                if (this.OnMessageOut != null)
                {
                    this.OnMessageOut(this, new Tuple<int, string>(2, total.ToString()));
                }

                return "";
            }
            catch (Exception ex) {
                timer.Stop();
                total = 0;
                return ex.Message;
            }
           


        }

        private void Blp_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            if (this.OnMessageOut != null) {
                this.OnMessageOut(this, new Tuple<int, string>(2, e.RowsCopied.ToString()));
            }
        }
    }
}

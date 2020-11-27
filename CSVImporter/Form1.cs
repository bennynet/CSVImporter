using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CSVImporter
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            im.OnMessageOut += Im_OnMessageOut;
        }

        private void Im_OnMessageOut(object sender, Tuple<int, string> e)
        {
            this.Invoke(new MethodInvoker(()=> {
                switch (e.Item1)
                {
                    case 0:
                        label8.Text = e.Item2;
                        break;
                    case 1:
                        label8.Text = "0/0";
                        progressBar1.Maximum = int.Parse(e.Item2);
                        break;
                    case 2:
                        label8.Text =string.Format("{0}/{1}",e.Item2,progressBar1.Maximum.ToString());
                        progressBar1.Value= int.Parse(e.Item2);
                        break;
                }
            }));
            
        }

        private bool mustHaveValue() {
            foreach (var c in this.Controls) {
                if (c is TextBox) {
                    TextBox tb = c as TextBox;
                    if (!tb.ReadOnly&& string.IsNullOrEmpty(tb.Text.Trim())) {
                        return false;
                    }
                }
            }

            return true;
        }

        ImporterMananger im = new ImporterMananger();
        private async void button2_Click(object sender, EventArgs e)
        {
            if (mustHaveValue())
            {
                button2.Text = "连接中..";
                button2.Enabled = false;
                string dbstr = getDBStr();
                string msg = await im.Ping(dbstr);
                MessageBox.Show(string.IsNullOrEmpty(msg) ? "连接成功！" : msg);
                button2.Text = "测试连接";
                button2.Enabled = true;
            }
            else {
                MessageBox.Show("请将数据库信息填写完整");
            }
           
        }

        private string getDBStr() {
            return string.Format("server={0};database={1};uid={2};pwd={3}", textBox2.Text.Trim(), textBox5.Text.Trim(), textBox3.Text.Trim(),textBox4.Text.Trim());
        }

        private async void button3_Click(object sender, EventArgs e)
        {
            if (File.Exists(textBox1.Text.Trim()))
            {
                if (!mustHaveValue()) {
                    MessageBox.Show("请将数据库信息填写完整！");
                    return;
                }
                button3.Text = "导入中....";
                button3.Enabled = false;

                string dbstr = getDBStr();

                string filepath = textBox1.Text.Trim();

                Stopwatch sw = new Stopwatch();
                sw.Start();

                string msg = await im.ImportAsync(dbstr, filepath, textBox6.Text.Trim());

                sw.Stop();

                MessageBox.Show(string.IsNullOrEmpty(msg) ? string.Format( "导入成功！共耗时:{0}秒",sw.Elapsed.Seconds.ToString()) : msg);

                button3.Text = "开始导入";
                button3.Enabled = true;


            }
            else {
                MessageBox.Show("csv文件不存在");
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            using (OpenFileDialog of = new OpenFileDialog()) {
                of.Filter = "*.csv|*.csv";
                if (of.ShowDialog() == DialogResult.OK) {
                    textBox1.Text = of.FileName;
                }
            }
        }
    }
}

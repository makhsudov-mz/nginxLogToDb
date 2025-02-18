using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApp3
{
	public partial class Form1 : Form
	{
		MySqlConnection con = new MySqlConnection("server=localhost; database=web_api_access; user=root; password='';charset=utf8mb4;");

		static Mutex mutex = new Mutex();

		public Form1()
		{
			InitializeComponent();
		}
		private void button1_Click(object sender, EventArgs e)
		{
			if (openFileDialog1.ShowDialog() == DialogResult.OK)
				label2.Text = openFileDialog1.FileName;
		}

		int count = 0;
		private async void button2_Click(object sender, EventArgs e)
		{
			var stopwatch = new Stopwatch();
			stopwatch.Start();
			con.Open();
			label1.Text = "В процессе...";
			await InsertAsync(label2.Text);
			//await InsertAsync(path2);

			con.Close();
			stopwatch.Stop();
			label1.Text = $"Завершено. Обработано {count} строк | Прошло {stopwatch.Elapsed}";
		}

		private async Task InsertAsync(string pathfile)
		{
			using (var reader = new StreamReader(pathfile))
			{
				string line;
				while ((line = await reader.ReadLineAsync()) != null)
				{
					await Task.Run(async () =>
					{
						using (MySqlCommand cmd = new MySqlCommand("INSERT INTO infotable(ip,unk1,unk2,date_t,zapros,res,raz,unk3,ustroystvo,unk4)"
						+ "VALUES(@ip,@unk1,@unk2,@date_t,@zapros,@res,@raz,@unk3,@ustroystvo,@unk4)", con))
						{
							try
							{
								line = line.Replace("\"", "");
								string[] arline = line.Split(' ', (char)StringSplitOptions.RemoveEmptyEntries);

								arline[3] = arline[3].Replace('/', ':');
								string[] datestr = arline[3].Split(':');

								DateTime dateTime = Convert.ToDateTime($"{datestr[0].Substring(1)}.{datestr[1]}.{datestr[2]} {datestr[3]}:{datestr[4]}:{datestr[5]}");

								cmd.Parameters.Add("@ip", MySqlDbType.VarChar).Value = arline[0];
								cmd.Parameters.Add("@unk1", MySqlDbType.VarChar).Value = arline[1];
								cmd.Parameters.Add("@unk2", MySqlDbType.VarChar).Value = arline[2];
								cmd.Parameters.Add("@date_t", MySqlDbType.VarChar).Value = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
								cmd.Parameters.Add("@zapros", MySqlDbType.VarChar).Value = $"{arline[5]} {arline[6]} {arline[7]}";
								cmd.Parameters.Add("@res", MySqlDbType.VarChar).Value = arline[8];
								cmd.Parameters.Add("@raz", MySqlDbType.VarChar).Value = arline[9];
								cmd.Parameters.Add("@unk3", MySqlDbType.VarChar).Value = arline[10];
								cmd.Parameters.Add("@ustroystvo", MySqlDbType.VarChar).Value = arline[11];
								cmd.Parameters.Add("@unk4", MySqlDbType.VarChar).Value = arline[12];
								cmd.ExecuteNonQuery();
							}
							catch (Exception ex)
							{
								mutex.WaitOne();
								using (StreamWriter sw = new StreamWriter("logs/log.txt"))
								{
									await sw.WriteLineAsync(ex.Message);
								}
								mutex.ReleaseMutex();
							}
						}
					});
					count++;
				}
			}

		}


	}
}

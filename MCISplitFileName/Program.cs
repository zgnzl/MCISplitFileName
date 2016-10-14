using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TPI;

namespace MCISplitFileName
{

    class Program
    {
        #region 全局变量
        private static string strUserIp = "127.0.0.1";
        private static string strIP = "192.168.105.23";
        private static int nPort = 4567;
        private static string strUserName = "DBOWN";
        private static string strPassword = "";
        private static string directorypath = @"C:\Users\NZL\Desktop\MCI样本数据抽取\结果集\MCI拆分大文件名";
        private static string recdirectorypath = @"C:\Users\NZL\Desktop\MCI样本数据抽取\结果集\REC";
        private static string[] resource = { "CHKJ", "CDMH", "CHKP", "CHKN", "BSFD", "BSAD", "BSPD", "BSSD", "BSSF", "BSHF", "PMMB" };
        private static char[] resourceType = { 'A', 'B', 'C', 'D' };
        #endregion
        static void Main(string[] args)
        {
            // SplitMCI(directorypath);
            // ExcuteKbasesql(Path.Combine(directorypath,"result"));
            // CombineRec(recdirectorypath);

           //  SplitMCI(@"C:\Users\NZL\Desktop\新建文件夹");
           // ExcuteKbasesql(Path.Combine(@"C:\Users\NZL\Desktop\新建文件夹", "result"));
            CombineRec(@"C:\Users\NZL\Desktop\新建文件夹\REC");
            Console.WriteLine("已完成！");
            Console.Read();
        }

        /// <summary>
        /// 拆分大的文件名（kbase最大支持10K）
        /// </summary>
        /// <param name="directorypath">文件夹路径</param>
        private static void SplitMCI(string directorypath)
        {
            DirectoryInfo directoryinfo = new DirectoryInfo(directorypath);
            Parallel.ForEach(directoryinfo.GetFiles(), (fileinfo) =>
            {
                int TOTAL = 0;
                string filename = fileinfo.Name;
                string directory = fileinfo.DirectoryName;
                string filepath = fileinfo.FullName;
                string[] kbasesql = File.ReadAllLines(filepath, Encoding.GetEncoding("GB2312"));
                string[] fileValue = kbasesql[1].Split('+');
                int index = 0;
                int everytotal = 0;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < fileValue.Length; i++)
                {
                    TOTAL++; everytotal++;
                    if (i > 0 && i % 680 == 0)
                    {
                        index++;
                        sb.Append(fileValue[i] + "+");
                        StreamWriter sw = new StreamWriter(Path.Combine(directory, "result", filename.Replace(".txt", index + "-" + everytotal + ".txt")), false, Encoding.GetEncoding("GB2312"));
                        sw.WriteLine(kbasesql[0]);
                        sw.WriteLine(sb.ToString().Trim('+'));
                        sw.WriteLine("  group by 文件名 ");
                        sw.WriteLine(kbasesql[2].Replace(".txt", index + ".txt"));
                        sw.Close();
                        sb.Clear();
                        Console.WriteLine(fileinfo.Name + index + "___成功:" + index);
                        everytotal = 0;
                    }
                    else
                    {
                        sb.Append(fileValue[i] + "+");
                    }
                    if (filename == "CHKP_A.txt" && TOTAL == 7140)
                    {
                        break;
                    }
                    if (filename == "CHKP_B.txt" && TOTAL == 3570)
                    {
                        break;
                    }
                    if (filename == "CHKP_C.txt" && TOTAL == 595)
                    {
                        break;
                    }
                    if (filename == "CHKP_D.txt" && TOTAL == 595)
                    {
                        break;
                    }
                }
                if (!string.IsNullOrWhiteSpace(sb.ToString().Trim('+')))
                {
                    StreamWriter sw = new StreamWriter(Path.Combine(directory, "result", filename.Replace(".txt", ++index + "-" + everytotal + ".txt")), false, Encoding.GetEncoding("GB2312"));
                    sw.WriteLine(kbasesql[0]);
                    sw.WriteLine(sb.ToString().Trim('+'));
                    sw.WriteLine("  group by 文件名 ");
                    sw.WriteLine(kbasesql[2]);
                    sw.Close();
                    sb.Clear();
                    Console.WriteLine(fileinfo.Name + index + "___成功:" + index);
                }
                Console.WriteLine(filename + "共计:" + TOTAL);
            });
        }

        /// <summary>
        /// 导出rec文件
        /// </summary>
        /// <param name="directorypath">文件夹路径</param>
        private static void ExcuteKbasesql(string directorypath)
        {
            Dictionary<string, int> dic = new Dictionary<string, int>();
            Hashtable hashtable = new Hashtable();
            DirectoryInfo directoryinfo = new DirectoryInfo(directorypath);
            IEnumerable<IGrouping<string, FileInfo>> Iegroupfileinfo = directoryinfo.GetFiles().GroupBy(c => c.Name.Substring(0, 6));
            foreach (IGrouping<string, FileInfo> group in Iegroupfileinfo)
            {
                dic.Add(group.Key, 0);
            }
            Client conn = GetConn();
            Parallel.ForEach(directoryinfo.GetFiles(), (fileinfo) =>
            {
                string kbasesql = File.ReadAllText(fileinfo.FullName, Encoding.GetEncoding("GB2312"));
                RecordSet rsSet = conn.OpenRecordSet(kbasesql);
                dic[fileinfo.Name.Substring(0, 6)] += rsSet.GetCount();
                if (rsSet.GetCount() < 1)
                {
                    Console.WriteLine(fileinfo.Name + "___失败:" + rsSet.GetCount());
                }
                else
                {
                    Console.WriteLine(fileinfo.Name + "___成功:" + rsSet.GetCount());
                }
                hashtable.Add(fileinfo.Name, "___成功:" + rsSet.GetCount());
            });

            StreamWriter sw = new StreamWriter(Path.Combine(directorypath,"log", "导出REC文件处理结果数据.txt"), true, Encoding.GetEncoding("GB2312"));
            foreach (DictionaryEntry dicentry in hashtable)
            {
                sw.WriteLine(dicentry.Key + "___成功:" + dicentry.Value);
            }
            sw.WriteLine("_______________________________________________________");
            foreach (KeyValuePair<string, int> keyval in dic)
            {
                Console.WriteLine(keyval.Key + ":" + keyval.Value);
                sw.WriteLine(keyval.Key + ":" + keyval.Value);
            }
            sw.Close();
        }

        /// <summary>
        /// 合并REC文件
        /// </summary>
        /// <param name="directorypath">文件路径</param>
        private static void CombineRec(string directorypath)
        {
            DirectoryInfo directoryinfo = new DirectoryInfo(directorypath);
            IEnumerable<IGrouping<string, FileInfo>> Iegroupfileinfo = directoryinfo.GetFiles().GroupBy(c => c.Name.Substring(0, 4));
              Parallel.ForEach(Iegroupfileinfo, (groupfileinfo) =>
            // foreach (IGrouping<string, FileInfo> groupfileinfo in Iegroupfileinfo)
             {
                 IEnumerable<FileInfo> fileinfochild = groupfileinfo.AsEnumerable<FileInfo>();
                 IEnumerable<IGrouping<string, FileInfo>> Iegroupfileinfochile = fileinfochild.GroupBy(c => c.Name.Substring(0, 6));
                 foreach (IGrouping<string, FileInfo> groupfileinfochild in Iegroupfileinfochile)
                 {
                     string type = groupfileinfochild.Key.Substring(5, 1);
                     IEnumerable<FileInfo> IEfileinfo = groupfileinfochild.AsEnumerable<FileInfo>();
                     foreach (FileInfo fileinfo in IEfileinfo)
                     {
                         Console.WriteLine("处理:" + fileinfo.Name);
                         StreamReader fs = new StreamReader(fileinfo.FullName, Encoding.GetEncoding("GB2312"));
                         if ((new FileInfo(fileinfo.FullName)).Length > 168168888)
                         {
                             char[] c = null;
                             StreamWriter sw = new StreamWriter(Path.Combine(directorypath, "RECresult", groupfileinfo.Key + ".txt"), true, Encoding.GetEncoding("GB2312"));
                             while (fs.Peek() >= 0)
                             {
                                 c = new char[16816888];
                                 fs.Read(c, 0, c.Length);
                                 string childvalue = string.Concat<char>(c);
                                 string value = childvalue.Replace("<REC>", "<REC><type>=" + type);
                                 sw.WriteLine(value);
                             }
                             fs.Close();
                             sw.Close();
                         }
                         else
                         { 
                         string filevalue = fs.ReadToEnd();
                         string value = filevalue.Replace("<REC>", "<REC><type>=" + type);
                         StreamWriter sw = new StreamWriter(Path.Combine(directorypath, "RECresult", groupfileinfo.Key + ".txt"), true, Encoding.GetEncoding("GB2312"));
                         sw.WriteLine(value);
                         fs.Close();
                         sw.Close();
                         }
                     }
                 }
              //    }
              });
        }

        /// <summary>
        /// 链接kbase
        /// </summary>
        /// <returns></returns>
        private static Client GetConn()
        {
            Client conn = new Client();
            conn.Connect(strIP, nPort, strUserName, strPassword, strUserIp);
            return conn;
        }
    }
}

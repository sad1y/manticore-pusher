using System;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace ManticorePusher
{
    public class Pusher
    {
        private const int TotalParameters = TextParameters + StringParameters + UintParameters + FloatParameters + 1;

        private const int TextParameters = 37;
        private const int StringParameters = 1;
        private const int UintParameters = 6;
        private const int FloatParameters = 1;

        private readonly int _workerCount;
        private readonly int _iteration;
        private readonly int _batchSize;
        private readonly TextGenerator _textGenerator;

        private readonly Random _rng = new();

        public Pusher(int workerCount, int iteration, int batchSize)
        {
            _workerCount = workerCount;
            _iteration = iteration;
            _batchSize = batchSize;
            _textGenerator = new TextGenerator("./words.txt");
        }

        public async Task Run()
        {
            var workers = new Task[_workerCount];

            for (var i = 0; i < _workerCount; i++)
            {
                var num = i;
                workers[i] = Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        await using var connection = new MySqlConnection("Server=127.0.0.1;Port=9306;SslMode=None;");
                        connection.Open();
                        CreateDb(connection, num);

                        long id = 1_000_000_000;

                        var batchCommand = CreateBatchCommand(connection, num, _batchSize);
                        for (var j = 0; j < _iteration; j++)
                        {
                            await InsertBatch(batchCommand, ref id, _batchSize);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            await Task.WhenAll(workers);
        }

        private Task InsertBatch(MySqlCommand cmd, ref long id, int batchSize)
        {
            for (var i = 0; i < batchSize; i++)
            {
                cmd.Parameters[i * TotalParameters].Value = id++;
                var offset = 1;

                for (var j = 0; j < TextParameters; j++)
                    cmd.Parameters[i * TotalParameters + j + offset].Value =
                        _textGenerator.GetText(16 + (int)Math.Pow(2, j / 4.5));

                offset += TextParameters;

                for (var j = 0; j < StringParameters; j++)
                {
                    cmd.Parameters[i * TotalParameters + j + offset].Value =
                        "[0,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-0.34,0.37127,0.34186,0.32774,-0.34,0.24423,0.15291,0.07984,-0.34,0.50611,0.44175,0.45259,-1,-1,0,-1.17585,-1,-1,0,0,-0.29441,-1,12.60748,0.9337,7.91793,0.95464,0.8467,1.66009,8.08743,0.96005,0.83967,1.74471,0.1695,0.81602,-0.06847,0.04297,0.64865,1.48806,-1,-1,-1,-1,-1,-1,-1,-1,-1,-16.18133,0.05308,-0.8039,-1.70807,-16.18133,0.0506,-0.83964,-1.74247,-16.18133,0.05616,-0.75965,-1.64769,-1,-1,-1,-1,-1,-1,-15.84232,0.05045,-0.91314,-1.72457,-15.84232,0.0527,-0.87564,-1.68718,-15.84232,0.0497,-0.92257,-1.72969,-1,-1,-1,-1,-1,-1,-1,-1,-1,0,0,-0.03229,-3.54672,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0,0,0]";
                }

                offset += StringParameters;

                for (var j = 0; j < UintParameters; j++)
                    cmd.Parameters[i * TotalParameters + j + offset].Value = (uint)_rng.Next(1000000, int.MaxValue);

                offset += UintParameters;

                for (var j = 0; j < FloatParameters; j++)
                    cmd.Parameters[i * TotalParameters + j + offset].Value = _rng.NextSingle();
            }

            return cmd.ExecuteNonQueryAsync();
        }

        private static MySqlCommand CreateBatchCommand(MySqlConnection connection, int tableNum, int batchSize)
        {
            var cmd = connection.CreateCommand();

            var sb = new StringBuilder();

            sb.Append("INSERT INTO table_");
            sb.Append(tableNum);
            sb.Append(" (");
            sb.Append("id,");
            
            
            for (var j = 0; j < TextParameters; j++)
            {
                sb.Append("text_");
                sb.Append(j);
                sb.Append(',');
            }
            
            for (var j = 0; j < StringParameters; j++)
            {
                sb.Append("string_");
                sb.Append(j);
                sb.Append(',');
            }
            
            for (var j = 0; j < UintParameters; j++)
            {
                sb.Append("uint_");
                sb.Append(j);
                sb.Append(',');
            }

            for (var j = 0; j < FloatParameters; j++)
            {
                sb.Append("float_");
                sb.Append(j);
                sb.Append(',');
            }

            sb.Remove(sb.Length - 1, 1);
            sb.Append(')');

            sb.Append(" VALUES ");

            for (var i = 0; i < batchSize; i++)
            {
                sb.Append('(');

                var idParamName = $"@p{i * TotalParameters}";
                sb.Append(idParamName); // id
                sb.Append(',');
                cmd.Parameters.Add(idParamName, MySqlDbType.Int64);

                var offset = 1;

                for (var j = 0; j < TextParameters; j++)
                {
                    var paramName = $"@p{i * TotalParameters + j + offset}";
                    sb.Append(paramName);
                    sb.Append(',');
                    cmd.Parameters.Add(paramName, MySqlDbType.String);
                }

                offset += TextParameters;

                for (var j = 0; j < StringParameters; j++)
                {
                    var paramName = $"@p{i * TotalParameters + j + offset}";
                    sb.Append(paramName);
                    sb.Append(',');
                    cmd.Parameters.Add(paramName, MySqlDbType.String);
                }

                offset += StringParameters;

                for (var j = 0; j < UintParameters; j++)
                {
                    var paramName = $"@p{i * TotalParameters + j + offset}";
                    sb.Append(paramName);
                    sb.Append(',');
                    cmd.Parameters.Add(paramName, MySqlDbType.UInt32);
                }

                offset += UintParameters;

                for (var j = 0; j < FloatParameters; j++)
                {
                    var paramName = $"@p{i * TotalParameters + j + offset}";
                    sb.Append(paramName);
                    sb.Append(',');
                    cmd.Parameters.Add(paramName, MySqlDbType.Float);
                }
                sb.Remove(sb.Length - 1, 1);

                sb.Append(')');
                sb.Append(',');
            }

            sb.Remove(sb.Length - 1, 1);

            cmd.CommandText = sb.ToString();

            return cmd;
        }

        private void CreateDb(MySqlConnection connection, int i)
        {
            var query = new StringBuilder($"CREATE TABLE IF NOT EXISTS ");
            query.Append("table_");
            query.Append(i);
            query.Append(" (");
            
            for (var j = 0; j < TextParameters; j++)
            {
                query.Append("text_");
                query.Append(j);
                query.Append(" text stored indexed,");
            }
            
            for (var j = 0; j < StringParameters; j++)
            {
                query.Append("string_");
                query.Append(j);
                query.Append(" string,");
            }
            
            for (var j = 0; j < UintParameters; j++)
            {
                query.Append("uint_");
                query.Append(j);
                query.Append(" uint,");
            }

            for (var j = 0; j < FloatParameters; j++)
            {
                query.Append("float_");
                query.Append(j);
                query.Append(" float,");
            }
            query.Remove(query.Length - 1, 1);
            query.Append(')');

            var cmd = connection.CreateCommand();
            cmd.CommandText = query.ToString();
            cmd.ExecuteNonQuery();
        }
    }
}
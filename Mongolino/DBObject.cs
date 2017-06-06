using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Mongolino
{
    public class DBObject<T> where T : DBObject<T>
    {
        public ObjectId _id { get; set; }

        public string FullText
        {
            get
            {
                var enu = GetType()
                           .GetProperties()
                           .Where(x => x.PropertyType == typeof(string) && x.Name != "FullText")
                           .SelectMany(x => (x.GetValue(this) as string)?.Split(Helper.Removable.Value) ?? new string[0])
                           .Distinct(StringComparer.InvariantCultureIgnoreCase)
                           .OrderByDescending(x => x);

                return string.Join(" ", enu);
            }
            set
            {
            }
        }

        #region STATICS

        public static string ConnectionString { get; set; }

        public static string Database { get; set; } = "default";

        public static string Collection { get; set; } = typeof(T).Name.ToLowerInvariant();

        static readonly Lazy<IMongoCollection<T>> _collection = new Lazy<IMongoCollection<T>>(()=> 
        {
            var client = ConnectionString != null ? new MongoClient(ConnectionString) : new MongoClient();
            var collection = client.GetDatabase(Database).GetCollection<T>(Collection);
            collection.Indexes.CreateOne(Builders<T>.IndexKeys.Text(x => x.FullText));
            return collection;
        });

        static IMongoCollection<T> MongoCollection => _collection.Value;

        public static void AscendingIndex(Expression<Func<T, object>> func)
        {
            MongoCollection.Indexes.CreateOne(Builders<T>.IndexKeys.Ascending(func));
        }

        public static void DescendingIndex(Expression<Func<T, object>> func)
        {
            MongoCollection.Indexes.CreateOne(Builders<T>.IndexKeys.Descending(func));
        }

        public static IEnumerable<T> Text(string text)
        {
            var filter = Builders<T>.Filter.Text(text);
            var res = MongoCollection.Find(filter);
            return res.ToEnumerable();
        }

        public static T Random
        {
            get
            {
                if (Empty) return default(T);

                var res = MongoCollection.Find(Builders<T>.Filter.Empty);

                var rnd = new Random(Guid.NewGuid().GetHashCode());

                return res.Skip(rnd.Next((int)res.Count() - 1)).FirstOrDefault();
            }
        }

        public static IEnumerable<T> All => MongoCollection.Find(Builders<T>.Filter.Empty).ToEnumerable();

        public static bool Empty => Count() == 0;


        public static IEnumerable<T> SortBy(Expression<Func<T, object>> sel)
        {
            var filter = Builders<T>.Filter.Empty;
            var res = MongoCollection.Find(filter).SortBy(sel);
            return res.ToEnumerable();
        }

        public static IEnumerable<T> SortByDescending(Expression<Func<T, object>> sel)
        {
            var filter = Builders<T>.Filter.Empty;
            var res = MongoCollection.Find(filter).SortByDescending(sel);
            return res.ToEnumerable();
        }

        public static bool Any<K>(Expression<Func<T,K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(sel,value);
            var res = MongoCollection.Find(filter);
            return res.Any();
        }

        public static async Task<bool> AnyAsync<K>(Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(sel, value);
            var res = await MongoCollection.FindAsync(filter);
            return await res.AnyAsync();
        }

        public static long Count()
        {
            return MongoCollection.Count(Builders<T>.Filter.Empty);
        }

        public static long Count<K>(Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(sel, value);
            return MongoCollection.Count(filter);
        }

        public static void Add(T obj)
        {
            MongoCollection.InsertOneAsync(obj);
        }

        public static T Create(T obj)
        {
            MongoCollection.InsertOne(obj);
            return obj;
        }

        public static T CreateAsync(T obj)
        {
            MongoCollection.InsertOneAsync(obj);
            return obj;
        }

        public static void Replace(T obj)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            MongoCollection.ReplaceOne(filter, obj);
        }

        public static T AddToAsync<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, IEnumerable<K> value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSetEach(sel, value);
            MongoCollection.UpdateOneAsync(filter, update);
            return obj;
        }

        public static T AddToAsync<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSet(sel, value);
            MongoCollection.UpdateOneAsync(filter, update);
            return obj;
        }

        public static T AddTo<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, IEnumerable<K> value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSetEach(sel, value);
            MongoCollection.UpdateOne(filter, update);
            return obj;
        }

        public static T AddTo<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSet(sel, value);
            MongoCollection.UpdateOne(filter, update);
            return obj;
        }

        public static void Update<K>(T obj, Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.Set(sel, value);
            MongoCollection.UpdateOneAsync(filter, update);
        }

        public static void Append<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSet(sel, value);
            MongoCollection.UpdateOneAsync(filter, update);
        }

        public static void Increase<K>(T obj, Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.Inc(sel, value);
            MongoCollection.UpdateOne(filter, update);
        }

        public static async Task IncreaseAsync<K>(T obj, Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.Inc(sel, value);
            await MongoCollection.UpdateOneAsync(filter, update);
        }

        public static void Delete<K>(Expression<Func<T, K>> p, K value)
        {
            MongoCollection.DeleteOne(Builders<T>.Filter.Eq(p, value));
        }

        public static void Delete(T obj)
        {
            MongoCollection.DeleteOne(Builders<T>.Filter.Eq(x => x._id, obj._id));
        }

        public static void Delete(IEnumerable<T> obj)
        {
            MongoCollection.DeleteMany(Builders<T>.Filter.In(x => x._id, obj.Select(x=>x._id)));
        }

        public static void DeleteAsync(T obj)
        {
            MongoCollection.DeleteOneAsync(Builders<T>.Filter.Eq(x => x._id, obj._id));
        }

        public static void DeleteAsync(IEnumerable<T> obj)
        {
            MongoCollection.DeleteManyAsync(Builders<T>.Filter.In(x => x._id, obj.Select(x => x._id)));
        }

        public static T GetOrCreate<K>(Expression<Func<T,K>> sel, K p, T obj)
        {
            var f = FirstOrDefault(sel,p);

            return f == default(T) ?  Create(obj) : f;
        }

        public static T Get(string id)
        {
            var p = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq(x => x._id, p);
            return MongoCollection.Find(filter).FirstOrDefault();
        }

        public static T FirstOrDefault()
        {
            var res = MongoCollection.Find(Builders<T>.Filter.Empty);
            return res.FirstOrDefault();
        }

        public static T FirstOrDefault<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            var res = MongoCollection.Find(filter);
            return res.FirstOrDefault();
        }

        public static async Task<T> FirstOrDefaultAsync<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            var res = await MongoCollection.FindAsync(filter);
            return await res.FirstOrDefaultAsync();
        }

        public static T First<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            return MongoCollection.Find(filter).FirstOrDefault();
        }

        public static IEnumerable<T> Where<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            return MongoCollection.Find(filter).ToEnumerable();
        }

        public static IEnumerable<T> Search(string search)
        {
            var filter = Builders<T>.Filter.Text(search);
            return MongoCollection.Find(filter).ToEnumerable();
        }

        public static int Sum(Func<T,int> sum) => MongoCollection.Find(Builders<T>.Filter.Empty).ToEnumerable().Sum(sum);

        public static double Sum(Func<T, double> sum) => MongoCollection.Find(Builders<T>.Filter.Empty).ToEnumerable().Sum(sum);

        public static decimal Sum(Func<T, decimal> sum) => MongoCollection.Find(Builders<T>.Filter.Empty).ToEnumerable().Sum(sum);

        public static float Sum(Func<T, float> sum) => MongoCollection.Find(Builders<T>.Filter.Empty).ToEnumerable().Sum(sum);

        public static T Parse(string id)
        {
            var p = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq(x => x._id, p);
            return MongoCollection.Find(filter).FirstOrDefault();
        }

        #endregion
    }
}

using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

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
                           .SelectMany(x => (x.GetValue(this) as string).Split(Helper.Removable.Value))
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

        static readonly Lazy<IMongoCollection<T>> _collection = new Lazy<IMongoCollection<T>>(()=> 
        {
            var client = ConnectionString != null ? new MongoClient(ConnectionString) : new MongoClient();
            var collection = client.GetDatabase(Database).GetCollection<T>(typeof(T).Name.ToLowerInvariant());
            collection.Indexes.CreateOne(Builders<T>.IndexKeys.Text(x => x.FullText));
            return collection;
        });

        static IMongoCollection<T> Collection => _collection.Value;

        public static void AscendingIndex(Expression<Func<T, object>> func)
        {
            Collection.Indexes.CreateOne(Builders<T>.IndexKeys.Ascending(func));
        }

        public static void DescendingIndex(Expression<Func<T, object>> func)
        {
            Collection.Indexes.CreateOne(Builders<T>.IndexKeys.Descending(func));
        }

        public static T Random
        {
            get
            {
                if (Empty) return default(T);

                var res = Collection.Find(Builders<T>.Filter.Empty);

                var rnd = new Random(Guid.NewGuid().GetHashCode());

                return res.Skip(rnd.Next((int)res.Count() - 1)).FirstOrDefault();
            }
        }

        public static IEnumerable<T> All
        {
            get
            {
                return Collection.Find(Builders<T>.Filter.Empty).ToEnumerable();
            }
        }

        public static bool Empty => Count() == 0;

        public static long Count()
        {
            return Collection.Count(Builders<T>.Filter.Empty);
        }

        public static long Count<K>(Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(sel, value);
            return Collection.Count(filter);
        }

        public static void Add(T obj)
        {
            Collection.InsertOneAsync(obj);
        }

        public static T Create(T obj)
        {
            Collection.InsertOne(obj);
            return obj;
        }

        public static void Replace(T obj)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            Collection.ReplaceOne(filter, obj);
        }

        public static void Update<K>(T obj, Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.Set(sel, value);
            Collection.UpdateOneAsync(filter, update);
        }

        public static void Append<K>(T obj, Expression<Func<T, IEnumerable<K>>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.AddToSet(sel, value);
            Collection.UpdateOneAsync(filter, update);
        }

        public static void Increase<K>(T obj, Expression<Func<T, K>> sel, K value)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            var update = Builders<T>.Update.Inc(sel, value);
            Collection.UpdateOneAsync(filter, update);
        }

        public static void Delete<K>(T obj)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, obj._id);
            Collection.DeleteOneAsync(filter);
        }

        public static T Get(ObjectId id)
        {
            var filter = Builders<T>.Filter.Eq(x => x._id, id);
            return Collection.Find(filter).FirstOrDefault();
        }

        public static T Get(string id)
        {
            var p = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq(x => x._id, p);
            return Collection.Find(filter).FirstOrDefault();
        }

        public static T One<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            return Collection.Find(filter).FirstOrDefault();
        }

        public static T First<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            return Collection.Find(filter).FirstOrDefault();
        }

        public static IFindFluent<T, T> Where<K>(Expression<Func<T, K>> sel, K p)
        {
            var filter = Builders<T>.Filter.Eq(sel, p);
            return Collection.Find(filter);
        }

        public static IEnumerable<T> Search(string search)
        {
            var filter = Builders<T>.Filter.Text(search);
            return Collection.Find(filter).ToEnumerable();
        }

        public static T Parse(string id)
        {
            var p = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq(x => x._id, p);
            return Collection.Find(filter).FirstOrDefault();
        }

        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AES;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace mongoDb
{
    public class Collections
    {
        private static MongoClient mongo_Client;
        private static IMongoDatabase derka;
        internal static IMongoCollection<BillTailor> derkaDerka;
        internal static IMongoCollection<Connections> collCnxns;
        public static byte[] Bytes => Newtonsoft.Json.JsonConvert.DeserializeObject<byte[]>("\"U2Vhbkdsb3ZlcmlibWlibQ==\"");
        public string Json { get; }

        public Collections()
        {
            var mongoCnxnEncrypted = "+Qd4LKQKW+Zn+vSxTE43iVtRrug4S0fcfi7Pbcl9Cf5BQR5EtXil/R7X37QK63Ncj8oz2F8WMC3oI79mESbniFXTrXBpRxe5fz0xtoYJ4BbtM+daWQolFk1bcjZaMsJ2";
            var mongoCnxnPlainText = AesOperation.DecryptString(Bytes, mongoCnxnEncrypted);
            mongo_Client = new MongoClient(mongoCnxnPlainText);
            derka = mongo_Client.GetDatabase("derka");
            derkaDerka = derka.GetCollection<BillTailor>("derka");
            collCnxns = derka.GetCollection<Connections>("monkeys");
            var monkey = collCnxns.Find(mky => mky.Id == "648cd289254edcf1d1dcbbb5").FirstOrDefault();
            Json = monkey.Properties["O1LZOQ//j5vrvYklH0DUbg=="];
        }

        public bool Write(string plainTextConnections)
        {
            var keyEncrypted = AesOperation.EncryptString(Bytes, "key");
            var valEncrypted = AesOperation.EncryptString(Bytes, plainTextConnections);
            var filter = Builders<Connections>.Filter.Eq(mky => mky.Id, "648cd289254edcf1d1dcbbb5");
            var update = Builders<Connections>.Update.Set("ms", new Dictionary<string, string>() { { keyEncrypted, valEncrypted } });
            try
            {
                var result = collCnxns.UpdateOne(filter, update);
                return result.IsAcknowledged;
            }
            catch (MongoWriteException mwe)
            {
                Console.WriteLine(mwe.Message);
                //Debugger.Break();
                return false;
            }
        }
        public bool WriteOne(Dictionary<string, string> ms)
        {
            var monkey = new Connections(ms);
            return monkey.Insert();
        }
        public async Task<string> Insert_imgBillTailor(string fileName, string hdrTimeStamp, string flatString, string jsonTable)
        {
            var billTailorInsert = new BillTailor(fileName, hdrTimeStamp, flatString, jsonTable);
            await derkaDerka.InsertOneAsync(billTailorInsert);
            return billTailorInsert.Id;
        }
        public string Delete_imgBillTailor(string id)
        {
            var filter = Builders<BillTailor>.Filter.Where(i => i.Id != id);
            var result = derkaDerka.DeleteMany(filter);
            return result.ToJson();
        }
    }

    [BsonIgnoreExtraElements]
    internal class BillTailor 
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; private set; }

        [BsonElement("fn")]
        public string Filename { get; }

        [BsonElement("ff")]
        public string Flatfile { get; }

        [BsonElement("tbl")]
        public string Table { get; }

        [BsonElement("hdr")]
        public string HdrTimeStamp { get; }

        public BillTailor(string filename, string hdrTimeStamp, string flatfile, string table)
        {
            // 01234567890ABC_
            var encryptKey = hdrTimeStamp + "_";
            HdrTimeStamp = AesOperation.EncryptString(encryptKey, hdrTimeStamp);
            Filename = AesOperation.EncryptString(encryptKey, filename);
            Flatfile = AesOperation.EncryptString(encryptKey, flatfile);
            Table = AesOperation.EncryptString(encryptKey, table);
        }
        public string Insert()
        {
            try
            {
                Collections.derkaDerka.InsertOneAsync(this).Wait();
                return Id;
            }
            catch (AggregateException aggEx)
            {
                aggEx.Handle(x =>
                {
                    if (x is MongoWriteException mwx && mwx.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        // mwx.WriteError.Message contains the duplicate key error message
                        return true;
                    }
                    return false;
                });
            }
            return null;
        }
    }
    internal class Connections 
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonElement("ms")]
        public Dictionary<string, string> Properties { get; set; }

        public Connections(Dictionary<string, string> s)
        {
            var dictionaire = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in s)
                dictionaire[AesOperation.EncryptString(Collections.Bytes, kvp.Key)] = AesOperation.EncryptString(Collections.Bytes, kvp.Value);
            Properties = dictionaire;
        }
        public bool Insert()
        {
            try
            {
                Collections.collCnxns.InsertOneAsync(this).Wait();
                return true;
            }
            catch (AggregateException aggEx)
            {
                aggEx.Handle(x =>
                {
                    if (x is MongoWriteException mwx && mwx.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        // mwx.WriteError.Message contains the duplicate key error message
                        return true;
                    }
                    return false;
                });
            }
            return false;
        }
        public override string ToString() => Properties.ToString();
    }
}
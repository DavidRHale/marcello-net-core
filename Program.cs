using System;
using System.Collections.Generic;
using System.IO;
using MarcelloDB.Platform;
using MarcelloDB.Storage;

namespace MarcelloPlay
{
    public class FileStorageStreamProvider : IStorageStreamProvider
    {
        string RootPath { get; set; }

        Dictionary<string, IStorageStream> Streams { get; set; }

        public FileStorageStreamProvider(string rootPath)
        {
            this.RootPath = rootPath;
            this.Streams = new Dictionary<string, IStorageStream>();
        }

        #region IStorageStreamProvider implementation
        public IStorageStream GetStream(string streamName)
        {
            if (!this.Streams.ContainsKey(streamName))
            {
                this.Streams.Add(
                    streamName,
                    new FileStorageStream(System.IO.Path.Combine(this.RootPath, streamName)));
            }
            return this.Streams[streamName];
        }
        #endregion

        public void Dispose()
        {
            Dispose(true);
        }

        void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var stream in this.Streams.Values)
                {
                    ((FileStorageStream)stream).Dispose();
                }
            }
            GC.SuppressFinalize(this);
        }

        ~FileStorageStreamProvider()
        {
            Dispose(false);
        }
    }

    internal class FileStorageStream : IStorageStream, IDisposable
    {
        FileStream _backingStream;

        internal FileStorageStream(string filePath)
        {
            _backingStream = new FileStream(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite);
        }

        #region IStorageStream implementation
        public byte[] Read(long address, int length)
        {
            byte[] result = new byte[length];
            _backingStream.Seek(address, SeekOrigin.Begin);
            _backingStream.Read(result, 0, length);
            return result;
        }

        public void Write(long address, byte[] bytes)
        {
            _backingStream.Seek(address, SeekOrigin.Begin);
            _backingStream.Write(bytes, 0, (int)bytes.Length);
            _backingStream.Flush(true);
        }
        #endregion

        public void Dispose()
        {
            Dispose(true);
        }

        void Dispose(bool disposing)
        {
            if (disposing)
            {
                _backingStream.Close();
                _backingStream.Dispose();
            }

            GC.SuppressFinalize(this);
        }

        ~FileStorageStream()
        {
            Dispose(false);
        }
    }

    public class Platform : IPlatform
    {
        public IStorageStreamProvider CreateStorageStreamProvider(string rootPath)
        {
            return new FileStorageStreamProvider(rootPath);
        }
    }

    public class Person
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string Surname { get; set; }
        public IList<Address> Addresses  { get; set; }
    }

    public class Address
    {
        public string Id { get; set; }  
        public string City { get; set; }  
    }

    class Program
    {
        static void Main(string[] args)
        {
            var platform = new Platform();
            var dataPath = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal);

            //This code is platform independent so we can implement this in shared code.
            //                              Injecting the platform here
            //                                      ||
            var session = new MarcelloDB.Session(platform, dataPath);
            var personsFile = session["persons.dat"];
            var personsCollection = personsFile.Collection<Person, string>("persons", p => p.Id);

            var jonSnow = new Person
            {
                Id = "123",
                FirstName = "Jon",
                Surname = "Snow",
                Addresses = new List<Address>
                {
                    new Address {City = "Castle Black"},
                    new Address {City = "Winterfell"},
                }
            };

            personsCollection.Persist(jonSnow);
            var foundJon = personsCollection.Find("123");
            Console.WriteLine("Found Jon");
        }
    }
}

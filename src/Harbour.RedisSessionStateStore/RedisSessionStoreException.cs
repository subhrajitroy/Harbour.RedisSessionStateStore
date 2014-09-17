using System;

namespace Harbour.RedisSessionStateStore
{
    public class RedisSessionStoreException : Exception
    {
        public RedisSessionStoreException(string message) : base(message)
        {
        }
    }
}
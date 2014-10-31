using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Web.SessionState;
using System.Collections.Specialized;
using System.Web;
using System.Web.Configuration;
using ServiceStack.Redis;
using System.Configuration.Provider;
using System.IO;
using System.Configuration;
using ServiceStack.Redis.Support.Locking;

namespace Harbour.RedisSessionStateStore
{
    /// <summary>
    /// A SessionStateProvider implementation for Redis using the ServiceStack.Redis client.
    /// </summary>
    /// <example>
    /// In your web.config (with the <code>host</code> and <code>clientType</code>
    /// attributes being optional):
    /// <code>
    /// <![CDATA[
    ///   <system.web>
    ///     <sessionState mode="Custom" customProvider="RedisSessionStateProvider">
    ///       <providers>
    ///         <clear />
    ///         <add name="RedisSessionStateProvider" 
    ///              type="Harbour.RedisSessionStateStore.RedisSessionStateStoreProvider" 
    ///              host="localhost:6379" clientType="pooled" />
    ///       </providers>
    ///     </sessionState>
    ///   </system.web>
    /// ]]>
    /// </code>
    /// If you wish to use a custom <code>IRedisClientsManager</code>, you can 
    /// do the following in your <code>Global.asax.cs</code>:
    /// <code>
    /// <![CDATA[
    ///   private IRedisClientsManager clientManager;
    ///  
    ///   protected void Application_Start()
    ///   {
    ///       // Or use your IoC container to wire this up.
    ///       clientManager = new PooledRedisClientManager("localhost:6379");
    ///       RedisSessionStateStoreProvider.SetClientManager(clientManager);
    ///   }
    ///  
    ///   protected void Application_End()
    ///   {
    ///       clientManager.Dispose();
    ///   }
    /// ]]>
    /// </code>
    /// </example>
    public sealed class RedisSessionStateStoreProvider : SessionStateStoreProviderBase
    {
        private static IRedisClientsManager clientManagerStatic;
        private static RedisSessionStateStoreOptions options;
        private static object locker = new object();

        private readonly Func<HttpContext, HttpStaticObjectsCollection> staticObjectsGetter;
        private string name;

        private int sessionTimeoutMinutes;

        /// <summary>
        /// Gets the client manager for the provider.
        /// </summary>
        public IRedisClientsManager ClientManager { get { return clientManagerStatic; } }

        internal RedisSessionStateStoreProvider(Func<HttpContext, HttpStaticObjectsCollection> staticObjectsGetter)
        {
            this.staticObjectsGetter = staticObjectsGetter;
        }

        public RedisSessionStateStoreProvider()
        {
            staticObjectsGetter = ctx => SessionStateUtility.GetSessionStaticObjects(ctx);
        }

        /// <summary>
        /// Sets the client manager to be used for the session state provider. 
        /// This client manager's lifetime will not be managed by the RedisSessionStateProvider.
        /// However, if this is not set, a client manager will be created and
        /// managed by the RedisSessionStateProvider.
        /// </summary>
        /// <param name="clientManager"></param>
        public static void SetClientManager(IRedisClientsManager clientManager)
        {
            if (clientManager == null) throw new ArgumentNullException();
            if (clientManagerStatic != null)
            {
                throw new InvalidOperationException("The client manager can only be configured once.");
            }
            clientManagerStatic = clientManager;
        }

        public static void SetOptions(RedisSessionStateStoreOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            if (RedisSessionStateStoreProvider.options != null)
            {
                throw new InvalidOperationException("The options have already been configured.");
            }

            // Clone so that we don't allow references to be modified once 
            // configured.
            RedisSessionStateStoreProvider.options = new RedisSessionStateStoreOptions(options);
        }

        internal static void ResetClientManager()
        {
            clientManagerStatic = null;
        }

        internal static void ResetOptions()
        {
            options = null;
        }
        
        public override void Initialize(string name, NameValueCollection config)
        {
            if (String.IsNullOrWhiteSpace(name))
            {
                name = "AspNetSession";
            }

            this.name = name;
            
            var sessionConfig = (SessionStateSection)WebConfigurationManager.GetSection("system.web/sessionState");

            sessionTimeoutMinutes = (int)sessionConfig.Timeout.TotalMinutes;

            lock (locker)
            {
                if (options == null)
                {
                    SetOptions(new RedisSessionStateStoreOptions());
                }

                if (clientManagerStatic == null)
                {
                    clientManagerStatic = CreateClientManager(config);
                }
            }

            base.Initialize(name, config);
        }

        private IRedisClientsManager CreateClientManager(NameValueCollection config)
        {
            var host = config["host"];
            var clientType = config["clientType"];

            if (String.IsNullOrWhiteSpace(host))
            {
                throw new RedisSessionStoreException("Session state store host name is null or empty");
            }

            if (String.IsNullOrWhiteSpace(clientType))
            {
                clientType = "POOLED";
            }

            if (clientType.ToUpper() == "POOLED")
            {
                return new PooledRedisClientManager(host);
            }
            if (clientType.ToUpper() == "SENTINEL")
            {
                return GetSentinelAwareClientManager(config);
            }
            return new BasicRedisClientManager(host);
        }

        private static IRedisClientsManager GetSentinelAwareClientManager(NameValueCollection config)
        {
            var master = config["master"];

            if (String.IsNullOrEmpty(master))
            {
                throw new RedisSessionStoreException("Redis sentinel master name is null or empty");
            }

            var hosts = ResolveSentinelHosts(config);
            var sentinel = new RedisSentinel(hosts, master);
            var redisClientsManager = sentinel.Setup();
            return redisClientsManager;
        }

        private static IEnumerable<string> ResolveSentinelHosts(NameValueCollection config)
        {
            var commaDelimitedSentinelHosts = config["host"];
            if (String.IsNullOrEmpty(commaDelimitedSentinelHosts))
            {
                throw new RedisSessionStoreException("Redis sentinel hosts is null or empty");
            }

            return commaDelimitedSentinelHosts.Split(',');
        } 

        private IRedisClient GetClient()
        {
            return clientManagerStatic.GetClient();
        }
        
        /// <summary>
        /// Create a distributed lock for cases where more-than-a-transaction
        /// is used but we need to prevent another request from modifying the
        /// session. For example, if we need to get the session, mutate it and
        /// then write it back. We can't use *just* a transaction for this 
        /// approach because the data is returned with the rest of the commands!
        /// </summary>
        /// <param name="client"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        private DisposableDistributedLock GetDistributedLock(IRedisClient client, string key)
        {
            var lockKey = key + options.KeySeparator + "lock";
            return new DisposableDistributedLock(
                client, lockKey, 
                options.DistributedLockAcquisitionTimeoutSeconds.Value, 
                options.DistributedLockTimeoutSeconds.Value
            );
        }

        private string GetSessionIdKey(string id)
        {
            return name + options.KeySeparator + id;
        }

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            var key = GetSessionIdKey(id);
            using (var client = GetClient())
            {
                var state = new RedisSessionState()
                {
                    Timeout = timeout,
                    Flags = SessionStateActions.InitializeItem
                };

                UpdateSessionState(client, key, state);
            }
        }

        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
               staticObjectsGetter(context),
               timeout);
        }

        public override void InitializeRequest(HttpContext context)
        {
            
        }

        public override void EndRequest(HttpContext context)
        {
            
        }

        private void UseTransaction(IRedisClient client, Action<IRedisTransaction> action)
        {
            using (var transaction = client.CreateTransaction())
            {
                action(transaction);
                transaction.Commit();
            }
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            var key = GetSessionIdKey(id);
            using (var client = GetClient())
            {
                IRedisTransaction transaction = client.CreateTransaction();
                using (transaction)
                {
                    transaction.QueueCommand(c => c.ExpireEntryIn(key, TimeSpan.FromMinutes                                                    (sessionTimeoutMinutes)));
                }
            }
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            var key = GetSessionIdKey(id);
            using (var client = GetClient())
            {
                var stateRaw = client.GetAllEntriesFromHashRaw(key);

                UseTransaction(client, transaction =>
                {
                    RedisSessionState state;
                    if (RedisSessionState.TryParse(stateRaw, out state) && state.Locked && state.LockId == (int)lockId)
                    {
                        transaction.QueueCommand(c => c.Remove(key));
                    }
                });
            }
        }

        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            return GetItem(false, context, id, out locked, out lockAge, out lockId, out actions);
        }

        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            return GetItem(true, context, id, out locked, out lockAge, out lockId, out actions);
        }

        private SessionStateStoreData GetItem(bool isExclusive, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            locked = false;
            lockAge = TimeSpan.Zero;
            lockId = null;
            actions = SessionStateActions.None;
            SessionStateStoreData result = null;

            var key = GetSessionIdKey(id);
            using (var client = GetClient())
            {

                var stateRaw = client.GetAllEntriesFromHashRaw(key);

                RedisSessionState state;
                if (!RedisSessionState.TryParse(stateRaw, out state))
                {
                    return null;
                }

                actions = state.Flags;

                if (state.Locked)
                {
                    locked = true;
                    lockId = state.LockId;
                    lockAge = DateTime.UtcNow - state.LockDate;
                    return null;
                }

                if (isExclusive)
                {
                    locked = state.Locked = true;
                    state.LockDate = DateTime.UtcNow;
                    lockAge = TimeSpan.Zero;
                    lockId = ++state.LockId;
                }

                state.Flags = SessionStateActions.None;

                UseTransaction(client, transaction =>
                {
                    transaction.QueueCommand(c => c.SetRangeInHashRaw(key, state.ToMap()));
                    transaction.QueueCommand(c => c.ExpireEntryIn(key, TimeSpan.FromMinutes(state.Timeout)));
                });

                var items = actions == SessionStateActions.InitializeItem ? new SessionStateItemCollection() : state.Items;

                result = new SessionStateStoreData(items, staticObjectsGetter(context), state.Timeout);
            }

            return result;
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            using (var client = GetClient())
            {
                UpdateSessionStateIfLocked(client, id, (int)lockId, state =>
                {
                    state.Locked = false;
                    state.Timeout = sessionTimeoutMinutes;
                });
            }
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            using (var client = GetClient())
            {
                if (newItem)
                {
                    var state = new RedisSessionState()
                    {
                        Items = (SessionStateItemCollection)item.Items,
                        Timeout = item.Timeout,
                    };

                    var key = GetSessionIdKey(id);
                    UpdateSessionState(client, key, state);
                }
                else
                {
                    UpdateSessionStateIfLocked(client, id, (int)lockId, state =>
                    {
                        state.Items = (SessionStateItemCollection)item.Items;
                        state.Locked = false;
                        state.Timeout = item.Timeout;
                    });
                }
            }
        }

        private void UpdateSessionStateIfLocked(IRedisClient client, string id, int lockId, Action<RedisSessionState> stateAction)
        {
            var key = GetSessionIdKey(id);
            var stateRaw = client.GetAllEntriesFromHashRaw(key);
            RedisSessionState state;
            //check if you are the one who has taken the lock, if so then you can update it
            // the state.LockId == lockId should tell you if you are the owner
            if (RedisSessionState.TryParse(stateRaw, out state) && state.Locked && state.LockId == lockId)
            {
                stateAction(state);
                UpdateSessionState(client, key, state);
            }

        }

        private void UpdateSessionState(IRedisClient client, string key, RedisSessionState state)
        {
            UseTransaction(client, transaction =>
            {
                transaction.QueueCommand(c => c.SetRangeInHashRaw(key, state.ToMap()));
                transaction.QueueCommand(c => c.ExpireEntryIn(key, TimeSpan.FromMinutes(state.Timeout)));
            });
        }

        public override void Dispose()
        {
            //Doing Nothing as of now
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            // Redis < 2.8 doesn't easily support key expiry notifications.
            // As of Redis 2.8, keyspace notifications (http://redis.io/topics/notifications)
            // can be used. Therefore, if you'd like to support the expiry
            // callback and are using Redis 2.8, you can inherit from this
            // class and implement it.
            return false;
        }

    }
}

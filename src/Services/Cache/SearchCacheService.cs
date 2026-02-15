using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using GatewayService;

namespace Cross.Services.Cache;

/// <summary>
/// In-memory cache for search results to avoid repeated searches for similar vectors.
/// Uses vector hash as cache key.
/// </summary>
public class SearchCacheService
{
    private readonly ConcurrentDictionary<string, QueryResponseObject> _cache;
    private readonly int _maxCacheSize;
    private readonly ILogger<SearchCacheService>? _logger;
    private readonly object _evictionLock = new object();

    public SearchCacheService(int maxCacheSize = 10000, ILogger<SearchCacheService>? logger = null)
    {
        _maxCacheSize = maxCacheSize;
        _logger = logger;
        _cache = new ConcurrentDictionary<string, QueryResponseObject>();
    }

    /// <summary>
    /// Generates a cache key from a vector.
    /// </summary>
    private string GetCacheKey(float[] vector, string bucketString)
    {
        // Create a hash from vector and bucket string
        var combined = $"{bucketString}:{string.Join(",", vector.Select(v => v.ToString("F4")))}";
        var bytes = Encoding.UTF8.GetBytes(combined);
        var hash = SHA256.HashData(bytes);
        return Convert.ToBase64String(hash);
    }

    /// <summary>
    /// Gets a cached search result if available.
    /// </summary>
    public QueryResponseObject? GetCachedResult(float[] vector, string bucketString)
    {
        var key = GetCacheKey(vector, bucketString);
        if (_cache.TryGetValue(key, out var result))
        {
            _logger?.LogDebug($"Search cache HIT for vector hash: {key.Substring(0, 8)}...");
            return result;
        }
        return null;
    }

    /// <summary>
    /// Caches a search result.
    /// </summary>
    public void CacheResult(float[] vector, string bucketString, QueryResponseObject result)
    {
        var key = GetCacheKey(vector, bucketString);
        
        // Evict if cache is too large
        if (_cache.Count >= _maxCacheSize)
        {
            EvictOldest();
        }

        _cache.TryAdd(key, result);
        _logger?.LogDebug($"Cached search result for vector hash: {key.Substring(0, 8)}... (cache size: {_cache.Count})");
    }

    /// <summary>
    /// Evicts oldest entries (simple FIFO eviction).
    /// </summary>
    private void EvictOldest()
    {
        lock (_evictionLock)
        {
            if (_cache.Count < _maxCacheSize)
                return;

            // Remove 10% of cache (simple eviction)
            var toRemove = _maxCacheSize / 10;
            var keys = _cache.Keys.Take(toRemove).ToList();
            foreach (var key in keys)
            {
                _cache.TryRemove(key, out _);
            }
            _logger?.LogDebug($"Evicted {toRemove} entries from search cache");
        }
    }

    /// <summary>
    /// Clears the cache.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
        _logger?.LogInformation("Search cache cleared");
    }

    /// <summary>
    /// Gets current cache statistics.
    /// </summary>
    public (int Count, int MaxSize) GetStats()
    {
        return (_cache.Count, _maxCacheSize);
    }
}



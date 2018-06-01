<?php

namespace Yeebase\Cache\Datastore\Cache\Backend;

/**
 * This file is part of the Yeebase.Cache.Datastore package.
 *
 * (c) 2018 yeebase media GmbH
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Google\Cloud\Datastore\DatastoreClient;
use Neos\Cache\Backend\AbstractBackend as IndependentAbstractBackend;
use Neos\Cache\Backend\PhpCapableBackendInterface;
use Neos\Cache\Backend\RequireOnceFromValueTrait;
use Neos\Cache\Backend\TaggableBackendInterface;
use Neos\Cache\EnvironmentConfiguration;

/**
 * @api
 */
class DatastoreBackend extends IndependentAbstractBackend implements TaggableBackendInterface, PhpCapableBackendInterface
{
    use RequireOnceFromValueTrait;

    const OPERATOR_EQUALS = '=';

    /**
     * @var DatastoreClient
     */
    protected $datastoreClient;

    /**
     * @var string
     */
    protected $keyFilePath;

    /**
     * @var string
     */
    protected $entityKind;

    /**
     * @var integer
     */
    protected $compressionLevel = 0;

    /**
     * Constructs the datastore backend
     *
     * @param EnvironmentConfiguration $environmentConfiguration
     * @param array $options Configuration options - depends on the actual backend
     */
    public function __construct(EnvironmentConfiguration $environmentConfiguration, array $options)
    {
        parent::__construct($environmentConfiguration, $options);
        if ($this->datastoreClient === null) {
            $this->datastoreClient = new DatastoreClient([
                'keyFilePath' => FLOW_PATH_ROOT . '/' . $this->keyFilePath
            ]);
        }
    }

    /**
     * Saves data in the cache.
     *
     * @param string $entryIdentifier An identifier for this specific cache entry
     * @param string $data The data to be stored
     * @param array $tags Tags to associate with this cache entry. If the backend does not support tags, this option can be ignored.
     * @param integer $lifetime Lifetime of this cache entry in seconds. If NULL is specified, the default lifetime is used. "0" means unlimited lifetime.
     * @throws \RuntimeException
     * @return void
     * @api
     */
    public function set(string $entryIdentifier, string $data, array $tags = [], ?int $lifetime = null)
    {
        $this->remove($entryIdentifier);

        if ($lifetime === null) {
            $lifetime = $this->defaultLifetime;
        }

        $cacheIdentifier = $this->buildKey('entry:' . $entryIdentifier);

        $cacheEntry = $this->datastoreClient->entity(
            $this->entityKind,
            [
                'cacheIdentifier' => $cacheIdentifier,
                'creationDatetime' => time(),
                'ttlDatetime' => time() + $lifetime,
                'tags' => $tags,
                'data' => $this->compress($data)
            ],
            ['excludeFromIndexes' => ['data']]
        );

        $this->datastoreClient->insert($cacheEntry);
    }

    /**
     * Loads data from the cache.
     *
     * @param string $entryIdentifier An identifier which describes the cache entry to load
     * @return mixed The cache entry's content as a string or FALSE if the cache entry could not be loaded
     * @api
     */
    public function get(string $entryIdentifier)
    {
        $cacheIdentifier = $this->buildKey('entry:' . $entryIdentifier);

        $query = $this->createQuery();
        $query->filter('cacheIdentifier', self::OPERATOR_EQUALS, $cacheIdentifier)->limit(1);

        $result = $this->datastoreClient->runQuery($query);

        if (!$result->valid()) {
            return false;
        }

        return $this->uncompress((string)$result->current()->data);
    }

    /**
     * Checks if a cache entry with the specified identifier exists.
     *
     * @param string $entryIdentifier An identifier specifying the cache entry
     * @return boolean TRUE if such an entry exists, FALSE if not
     * @api
     */
    public function has(string $entryIdentifier): bool
    {
        $cacheIdentifier = $this->buildKey('entry:' . $entryIdentifier);

        $query = $this->createQuery();
        $query->filter('cacheIdentifier', self::OPERATOR_EQUALS, $cacheIdentifier)->limit(1);

        $result = $this->datastoreClient->runQuery($query);

        return $result->valid();
    }

    /**
     * Removes all cache entries matching the specified identifier.
     * Usually this only affects one entry but if - for what reason ever -
     * old entries for the identifier still exist, they are removed as well.
     *
     * @param string $entryIdentifier Specifies the cache entry to remove
     * @throws \RuntimeException
     * @return boolean TRUE if (at least) an entry could be removed or FALSE if no entry was found
     * @api
     */
    public function remove(string $entryIdentifier): bool
    {
        $cacheIdentifier = $this->buildKey('entry:' . $entryIdentifier);

        $query = $this->createQuery();
        $query->filter('cacheIdentifier', '=', $cacheIdentifier);

        $keysToDelete = [];
        $result = $this->datastoreClient->runQuery($query);

        if (!$result->valid()) {
            return false;
        }

        while ($result->valid()) {
            $keysToDelete[] = $result->current()->key();
            $this->datastoreClient->delete($result->current()->key());
            $result->next();
        }

        return true;
    }

    /**
     * Removes all cache entries of this cache
     *
     * The flush method will use the EVAL command to flush all entries and tags for this cache
     * in an atomic way.
     *
     * @throws \RuntimeException
     * @return void
     * @api
     */
    public function flush()
    {
        $query = $this->createQuery();
        $query->keysOnly();

        $pageIterator = $this->datastoreClient->runQuery($query)->iterateByPage();

        if (!$pageIterator->valid()) {
            return;
        }

        while ($pageIterator->valid()) {
            $resultsPerPage = $pageIterator->current();
            if ($resultsPerPage === null) {
                return;
            }

            $batchKeys = [];
            foreach ($resultsPerPage as $itemToDelete) {
                $batchKeys[] = $itemToDelete->key();
            }

            $this->datastoreClient->deleteBatch($batchKeys);
            $pageIterator->next();
        }
    }

    /**
     * @return void
     * @api
     */
    public function collectGarbage()
    {
        $query = $this->createQuery();
        $query->filter('ttlDatetime', '<=', time());

        $pageIterator = $this->datastoreClient->runQuery($query)->iterateByPage();

        if (!$pageIterator->valid()) {
            return;
        }

        while ($pageIterator->valid()) {
            $resultsPerPage = $pageIterator->current();

            if ($resultsPerPage === null) {
                return;
            }

            $batchKeys = [];
            foreach ($resultsPerPage as $itemToDelete) {
                $batchKeys[] = $itemToDelete->key();
            }

            $this->datastoreClient->deleteBatch($batchKeys);
            $pageIterator->next();
        }
    }

    /**
     * @param string $identifier
     * @return string
     */
    private function buildKey(string $identifier): string
    {
        return $this->cacheIdentifier . ':' . $identifier;
    }

    /**
     * Removes all cache entries of this cache which are tagged by the specified tag.
     *
     * @param string $tag The tag the entries must have
     * @throws \RuntimeException
     * @return integer The number of entries which have been affected by this flush
     * @api
     */
    public function flushByTag(string $tag): int
    {

        $query = $this->createQuery()->filter('tags', '=', $tag);
        $pageIterator = $this->datastoreClient->runQuery($query)->iterateByPage();

        if (!$pageIterator->valid()) {
            return 0;
        }

        $affected = 0;
        while ($pageIterator->valid()) {
            $resultsPerPage = $pageIterator->current();

            if ($resultsPerPage === null) {
                return $affected;
            }

            $batchKeys = [];
            foreach ($resultsPerPage as $itemToDelete) {
                $batchKeys[] = $itemToDelete->key();
            }

            $this->datastoreClient->deleteBatch($batchKeys);
            $pageIterator->next();
            $affected++;
        }

        return $affected;
    }

    /**
     * Finds and returns all cache entry identifiers which are tagged by the
     * specified tag.
     *
     * @param string $tag The tag to search for
     * @return array An array with identifiers of all matching entries. An empty array if no entries matched
     * @api
     */
    public function findIdentifiersByTag(string $tag): array
    {
        $query = $this->createQuery()->filter('tags', '=', $tag);

        $pageIterator = $this->datastoreClient->runQuery($query)->iterateByPage();

        if (!$pageIterator->valid()) {
            return [];
        }

        $identifiers = [];
        while ($pageIterator->valid()) {
            $resultsPerPage = $pageIterator->current();

            if ($resultsPerPage === null) {
                return $identifiers;
            }

            foreach ($resultsPerPage as $entity) {
                $identifiers[] = $entity->getProperty('cacheIdentifier');
            }
            $pageIterator->next();
        }

        return $identifiers;
    }

    /**
     * TODO: No return type declaration for now, as it needs to return false as well.
     * @param string $value
     * @return mixed
     */
    private function uncompress($value)
    {
        if (empty($value)) {
            return $value;
        }
        return $this->useCompression() ? gzdecode($value) : $value;
    }

    /**
     * @param string $value
     * @return string|boolean
     */
    private function compress(string $value)
    {
        return $this->useCompression() ? gzencode($value, $this->compressionLevel) : $value;
    }

    /**
     * @return boolean
     */
    private function useCompression(): bool
    {
        return $this->compressionLevel > 0;
    }

    /**
     * @return \Google\Cloud\Datastore\Query\Query
     */
    protected function createQuery()
    {
        return $this->datastoreClient->query()->kind($this->entityKind);
    }

    /**
     * Sets the default lifetime for this cache backend
     *
     * @param integer $lifetime Default lifetime of this cache backend in seconds. If NULL is specified, the default lifetime is used. 0 means unlimited lifetime.
     * @return void
     * @api
     */
    public function setDefaultLifetime(int $lifetime)
    {
        $this->defaultLifetime = $lifetime;
    }

    /**
     * @param string $keyFilePath
     */
    public function setKeyFilePath(string $keyFilePath): void
    {
        $this->keyFilePath = $keyFilePath;
    }

    /**
     * @param string $entityKind
     */
    public function setEntityKind(string $entityKind): void
    {
        $this->entityKind = $entityKind;
    }
}

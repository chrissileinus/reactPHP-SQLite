<?php
/*
 * Created on Wed Feb 23 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\SQLite;

/**
 * A pool of React\SQLite connections
 */
class Pool
{
  const CS_ROUND_ROBIN = 'round-robin';
  const CS_BY_LOAD = 'load';

  private static $pool = [];
  private static $poolSize;
  private static $poolPointer = 0;
  private static $poolRequestCounter = [];
  private static $poolConnectionSelector = self::CS_BY_LOAD;

  /**
   * Initialize the connections
   *
   * @param  string        $dbFile
   * @param  int           $poolSize
   * @param  [type]        $connectionSelector
   * @param  callable|null $onError
   * @param  string|null   $schemaFile
   * @return void
   */
  static function init(string $dbFile, int $poolSize = 5, string $connectionSelector = self::CS_BY_LOAD, callable $onError = null, string $schemaFile = null, array $pragma = [])
  {
    self::$poolSize = $poolSize;
    self::$poolConnectionSelector = $connectionSelector;

    $dbFileExist = file_exists($dbFile);

    self::$pool[0] = new Connection($dbFile, $onError, $pragma);
    self::$poolRequestCounter[0] = 0;

    if (!$dbFileExist && $schemaFile && file_exists($schemaFile)) {
      $sqlque = explode(";", preg_replace(
        [
          "/ ENGINE=\w+/",
          "/--.*\n/",
          "/\n/",
        ],
        [
          "",
          "",
          "",
        ],
        file_get_contents($schemaFile)
      ));
      $sqlque = array_filter($sqlque, function ($entry) {
        return !!strlen($entry);
      });

      $promises = [];
      foreach ($sqlque as $sql) {
        $promises[] = self::$pool[0]->query($sql)->then(function (\Clue\React\SQLite\Result $result) {
          echo "Query {$result->insertId} OK, {$result->changed} row(s) changed" . PHP_EOL;
        });
      }
      \React\Async\await(\React\Promise\all($promises));
    }

    for ($p = 1; $p < self::$poolSize; $p++) {
      self::$pool[$p] = new Connection($dbFile, $onError, $pragma);
      self::$poolRequestCounter[$p] = 0;
    }
  }

  static function statistic(): array
  {
    return [
      'size' => self::$poolSize,
      'counter' => self::$poolRequestCounter,
    ];
  }

  static private function shiftPointer()
  {
    self::$poolPointer = (self::$poolPointer + 1) % self::$poolSize;

    if (self::$poolConnectionSelector == self::CS_BY_LOAD) {
      if (self::$poolRequestCounter[self::$poolPointer] == 0) return self::$poolPointer;

      $rcList = self::$poolRequestCounter; // copy
      asort($rcList, SORT_NUMERIC);
      self::$poolPointer = key($rcList);
    }

    return self::$poolPointer;
  }

  static private function pooledCallbackPromise(callable $callback)
  {
    $pointer = self::shiftPointer();
    self::$poolRequestCounter[$pointer]++;
    $connection = self::$pool[$pointer];
    return $callback($connection)->then(function ($result) use ($pointer) {
      self::$poolRequestCounter[$pointer]--;
      return $result;
    });
  }

  /**
   * Performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $query
   * @return \React\Promise\PromiseInterface
   */
  static function query(string $sql): \React\Promise\PromiseInterface
  {
    return self::pooledCallbackPromise(function (Connection $connection) use ($sql) {
      return $connection->query($sql);
    });
  }

  /**
   * Ping the connection.
   *
   * This method returns a promise that will resolve (with a void value) on
   * success or will reject with an `Exception` on error.
   *
   * @return \React\Promise\PromiseInterface
   */
  static function ping(): \React\Promise\PromiseInterface
  {
    return self::pooledCallbackPromise(function (Connection $connection) {
      return $connection->ping();
    });
  }
}

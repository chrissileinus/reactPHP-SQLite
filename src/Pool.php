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

  private static $pageSize;
  private static $pageCount;
  private static $pageFreeCount;

  private static $onEvent = null;

  public static $estimatedQueryRuntime = null;

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
  static function init(string $dbFile, int $poolSize = 5, string $connectionSelector = self::CS_BY_LOAD, callable $onEvent = null, callable $onError = null, string $schemaFile = null, array $pragma = [], float $estimatedQueryRuntime = 1.0)
  {
    self::$poolSize = $poolSize;
    self::$poolConnectionSelector = $connectionSelector;
    self::$estimatedQueryRuntime = $estimatedQueryRuntime;

    self::$onEvent = $onEvent;

    $dbFileExist = file_exists($dbFile);

    self::$pool[0] = new Connection($dbFile, $onError, $pragma);
    self::$poolRequestCounter[0] = 0;

    self::onEvent("Init");

    // Create Database
    if (!$dbFileExist) {
      self::onEvent("Create Database");
      \React\Async\await(self::createFromSchema($schemaFile));
    }

    if ($schemaFile) {
      self::onEvent("Upgrade Schema");
      self::upgradeFromSchema($schemaFile);
    }


    self::onEvent("Open child processes");
    for ($pointer = 1; $pointer < self::$poolSize; $pointer++) {
      self::$pool[$pointer] = new Connection($dbFile, $onError, $pragma);
      self::$poolRequestCounter[$pointer] = 0;
    }

    for ($pointer = 0; $pointer < self::$poolSize; $pointer++) {
      self::$pool[$pointer]->on('query', function (float $inSeconds, string $sql) {
        self::onEvent(sprintf(
          "slow Query: done in %.2f sec.\n %s",
          $inSeconds,
          $sql
        ));
      });
    }

    self::onEvent(sprintf(
      "Opend: '%s' with %d child processes",
      $dbFile,
      self::$poolSize
    ));
  }

  static private function createFromSchema(string $schemaFile)
  {
    if (!$schemaFile) {
      throw new \Exception("No Schema provided", 1);
    }
    if (!file_exists($schemaFile)) {
      throw new \Exception("Schema '{$schemaFile}' not found", 1);
    }

    $sqlQueFile = self::sqlQueFromSchemaFile($schemaFile);

    self::onEvent("Create new database with schema '{$schemaFile}'");

    $promises = [];
    foreach ($sqlQueFile as $sql) {
      preg_match('/CREATE TABLE.+`(?<name>\w+)`\s+\(/', tools::trimSql($sql), $matches);
      $name = $matches['name'];
      $promises[] = self::$pool[0]->query($sql)->then(function () use ($name) {
        self::onEvent("Created new Table '{$name}'");
      });
    }
    $schemaFileMtime = filemtime($schemaFile);
    $promises[] = self::$pool[0]->query("PRAGMA user_version = {$schemaFileMtime}");
    return \React\Promise\all($promises);
  }

  static function upgradeFromSchema(string $schemaFile)
  {
    if ($schemaFile && file_exists($schemaFile)) {
      $currentSchemaVersion = self::getPragma("user_version");
      $schemaFileMtime = filemtime($schemaFile);

      if ($currentSchemaVersion == $schemaFileMtime) {
        self::onEvent("Current schema is up to date");
        return;
      }

      return; // do nothing more at the moment.

      $sqlQueFile = self::sqlQueFromSchemaFile($schemaFile);

      $sqlQueCurrent = self::sqlQueFromCurrentSchema();

      $promises = [];
      foreach ($sqlQueFile as $newSql) {
        $newSqlTrimed = tools::trimSql($newSql);
        if (!in_array($newSqlTrimed, $sqlQueCurrent) && preg_match('/CREATE TABLE.+`(?<name>\w+)`\s+\(/', $newSqlTrimed, $matches)) {
          $name = $matches['name'];

          var_dump([$name, $newSql]);

          if (!isset($sqlQueCurrent[$name])) {
            $promises[] = self::$pool[0]->query($newSql)->then(function (\Clue\React\SQLite\Result $result) use ($name) {
              self::onEvent("Created new Table '{$name}'");
            });
          } else {
            self::onEvent("Upgrade existing Table '{$name}' starting...");
            self::onEvent("Upgrade existing Table '{$name}' finnished!");
          }
        }
      }
      return \React\Async\await(\React\Promise\all($promises));
    }
  }

  static private function sqlQueFromSchemaFile(string $schemaFile)
  {
    if (!file_exists($schemaFile)) return;

    return array_filter(
      explode(";", preg_replace(
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
      )),
      function ($entry) {
        return !!strlen($entry);
      }
    );
  }

  static private function sqlQueFromCurrentSchema()
  {
    $current = [];

    $promises[] = self::$pool[0]->query("SELECT name, sql FROM sqlite_master WHERE ( type='table' AND name not like 'sqlite%' )")->then(function ($result) use (&$current) {
      foreach ($result->rows as $entry) {
        $current[$entry['name']] = tools::trimSql($entry['sql']);
      }
    });

    \React\Async\await(\React\Promise\all($promises));

    return $current;
  }

  static private function onEvent($message)
  {
    if (is_callable(self::$onEvent))
      call_user_func(self::$onEvent, $message);
    else
      echo $message . PHP_EOL;
  }

  static function getPragma(string $name)
  {
    return \React\Async\await(self::$pool[0]->query("PRAGMA $name")->then(function ($result) use ($name) {
      return $result->rows[0][$name];
    }));
  }

  static function statistic(): array
  {
    $stat = [
      'size' => self::$poolSize,
      'counter' => self::$poolRequestCounter,
      'pageSize' => self::$pageSize,
      'pageCount' => self::$pageCount,
      'pageFreeCount' => self::$pageFreeCount,
    ];

    self::query('PRAGMA page_size')->then(function ($result) {
      self::$pageSize       = $result->rows[0]['page_size'];
    });
    self::query('PRAGMA page_count')->then(function ($result) {
      self::$pageCount      = $result->rows[0]['page_count'];
    });
    self::query('PRAGMA freelist_count')->then(function ($result) {
      self::$pageFreeCount  = $result->rows[0]['freelist_count'];
    });

    return $stat;
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
    return $callback($connection, $pointer)->then(function ($result) use ($pointer) {
      self::$poolRequestCounter[$pointer]--;
      return $result;
    }, function ($e) use ($pointer) {
      self::$poolRequestCounter[$pointer]--;
      return $e;
    });
  }

  /**
   * Performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $query
   * @param  float                           $estimatedRuntime
   * @return \React\Promise\PromiseInterface
   */
  static function query(string $sql, float $estimatedRuntime = null): \React\Promise\PromiseInterface
  {
    if ($estimatedRuntime == null) $estimatedRuntime = self::$estimatedQueryRuntime;
    return self::pooledCallbackPromise(function (Connection $connection) use ($sql, $estimatedRuntime) {
      return $connection->query($sql, [], $estimatedRuntime);
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

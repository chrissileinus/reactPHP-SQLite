<?php
/*
 * Created on Fri Mar 04 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\SQLite;

use Clue\React\SQLite;

/**
 * A single React\SQLite connection
 */
class Connection implements \Evenement\EventEmitterInterface
{
  use \Evenement\EventEmitterTrait;

  protected $connection;
  protected $onError;

  const PRAGMAdefault = [
    'journal_mode' => 'WAL',
    'journal_size_limit' => '1000',
    'synchronous' => 'NORMAL',
    'busy_timeout' => '60000',
    'temp_store' => 'memory',
    'mmap_size' => '30000000000',
  ];

  function __construct(string $dbFile, callable $onError = null, array $pragma = [])
  {
    $this->onError = $onError;

    $sqlFactory = new SQLite\Factory();

    $this->connection = $sqlFactory->openLazy($dbFile);
    $this->connection->on('error', function ($e) {
      $this->emit('error', [$e]);
    });
    $this->connection->on('close', function () {
      $this->emit('close');
    });

    if (!$pragma) $pragma = [];
    foreach (self::PRAGMAdefault as $key => $value) {
      $pragma[$key] ??= $value;
    }

    foreach ($pragma as $name => $value) {
      $this->connection->exec("PRAGMA {$name} = {$value};");
    }
  }

  function __destruct()
  {
    $this->connection->exec('PRAGMA optimize;');
  }

  public function exec($sql): \React\Promise\PromiseInterface
  {
    return $this->connection->exec($sql)->then(
      null,
      function (\Throwable $th) use ($sql) {
        if (is_callable($this->onError)) {
          return call_user_func($this->onError, $th, $sql);
        }
        throw $th;
      }
    );
  }

  public function query($sql, $params = []): \React\Promise\PromiseInterface
  {
    return $this->connection->query($sql, $params)->then(
      function (SQLite\Result $result) {
        return $result;
      },
      function (\Throwable $th) use ($sql) {
        if (is_callable($this->onError)) {
          return call_user_func($this->onError, $th, $sql);
        }
        throw $th;
      }
    );
  }

  public function ping()
  {
    return $this->connection->query('PRAGMA encoding');
  }

  public function quit()
  {
    return $this->connection->quit();
  }

  public function close()
  {
    return $this->connection->close();
  }
}

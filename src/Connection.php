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

  function __construct(string $dbFile, callable $onError = null)
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
    $this->connection->exec('PRAGMA journal_mode = WAL;');
    $this->connection->exec('PRAGMA synchronous = NORMAL;');
    $this->connection->exec('PRAGMA busy_timeout = 5000;');
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

<?php
/*
 * Created on Wed Feb 23 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\SQLite;

use SQLite3;

class tools
{
  static function quote($data)
  {
    if (is_float($data) || is_int($data)) {
      return $data;
    }
    if (is_bool($data)) {
      return $data ? 1 : 0;
    }
    if (is_string($data)) {
      return '\'' . SQLite3::escapeString($data) . '\'';
    }
    if ($data) {
      throw new \Exception('Invalid data type.');
    }
    return 'NULL';
  }
}

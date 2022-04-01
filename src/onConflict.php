<?php
/*
 * Created on Wed Feb 23 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\SQLite;

enum onConflict
{
  case Nothing;
  case Ignore;
  case Update;
}

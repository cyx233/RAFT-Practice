package surfstore

import (
	"errors"
	"time"
)

const DEFAULT_META_FILENAME string = "index.db"

const TOMBSTONE_HASHVALUE string = "0"
const EMPTYFILE_HASHVALUE string = "-1"

const FILENAME_INDEX int = 0
const VERSION_INDEX int = 1
const HASH_LIST_INDEX int = 2

const CONFIG_DELIMITER string = ","
const HASH_DELIMITER string = " "

var OldVerError error = errors.New("Loacl Version < Server Version. Pull From Server")
var TIMEOUT = time.Second * 5
var HEARTBEAT_TIMEOUT = time.Millisecond * 500

package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `
		INSERT INTO indexes(
			fileName,
			version,
			hashIndex,
			hashValue
		)
		VALUES(?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		if e := os.Remove(outputMetaPath); e != nil {
			return e
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		return err
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		return err
	}
	statement.Exec()
	insertStm, err := db.Prepare(insertTuple)
	if err != nil {
		return err
	}
	for filename, v := range fileMetas {
		if v == nil {
			return errors.New(filename + ": MetaData == nil")
		}
		for i, hash := range v.GetBlockHashList() {
			insertStm.Exec(v.GetFilename(), v.GetVersion(), i, hash)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes`

const getTuplesByFileName string = `
	SELECT version, hashValue
	FROM indexes 
	WHERE fileName=?
	ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, e
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		return nil, err
	}
	stm, err := db.Prepare(getTuplesByFileName)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, err
		}
		tuples, err := stm.Query(filename)
		if err != nil {
			return nil, err
		}
		fileMeta := &FileMetaData{
			Filename:      filename,
			Version:       0,
			BlockHashList: make([]string, 0),
		}
		var hv string
		for tuples.Next() {
			if err := tuples.Scan(&fileMeta.Version, &hv); err != nil {
				return nil, err
			}
			fileMeta.BlockHashList = append(fileMeta.BlockHashList, hv)
		}
		fileMetaMap[filename] = fileMeta
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

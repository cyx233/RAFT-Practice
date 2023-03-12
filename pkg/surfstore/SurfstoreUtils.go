package surfstore

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) error {
	//Read Meta Data
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			WriteMetaFile(make(map[string]*FileMetaData), client.BaseDir)
		} else {
			return errors.New("Load Local Meta Error: " + err.Error())
		}
	}
	// log.Println("Local before Sync: ")
	// PrintMetaMap(localIndex)

	for {
		//Pull before Push. Sync First Win.
		if err := PullFromServer(client, localIndex); err != nil {
			return errors.New("Pull From Server Error: " + err.Error())
		}
		if err := PushToServer(client, localIndex); err != nil {
			if err == OldVerError {
				//Pull again
				continue
			} else {
				return errors.New("Push To Server Error: " + err.Error())
			}
		}
		break
	}

	// log.Println("Local after Sync: ")
	// PrintMetaMap(localIndex)
	//Write Meta Data
	if err := WriteMetaFile(localIndex, client.BaseDir); err != nil {
		return errors.New("Write Local Meta Error: " + err.Error())
	}
	return nil
}

func GetChunkedFile(path string, blockSize int) ([]string, []*Block, error) {
	var hashList []string
	var blockList []*Block
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	fReader := bufio.NewReaderSize(file, blockSize)
	for {
		chunck := make([]byte, blockSize)
		n, err := fReader.Read(chunck)
		if err == nil {
			hashList = append(hashList, GetBlockHashString(chunck[:n]))
			block := &Block{
				BlockSize: int32(n),
				BlockData: chunck[:n],
			}
			blockList = append(blockList, block)
		} else if err == io.EOF {
			break
		} else {
			return nil, nil, err
		}
	}
	return hashList, blockList, nil
}

func Download(client RPCClient, remoteMetaData *FileMetaData) error {
	if remoteMetaData == nil {
		return errors.New("fileMetaData == nil")
	}
	file, err := os.Create(ConcatPath(client.BaseDir, remoteMetaData.Filename))
	if err != nil {
		return err
	}
	defer file.Close()
	hashes := remoteMetaData.GetBlockHashList()
	hashAddrMap := make(map[string]string)
	if err := client.GetHashAddrMap(hashes, &hashAddrMap); err != nil {
		return err
	}

	for _, hash := range hashes {
		var block Block
		if err := client.GetBlock(hash, hashAddrMap[hash], &block); err != nil {
			return err
		}
		if _, err := file.Write(block.BlockData[:block.BlockSize]); err != nil {
			return err
		}
	}

	return nil
}

func PullFromServer(client RPCClient, localIndex map[string]*FileMetaData) error {
	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		return err
	}
	// log.Println("Remote before Sync: ")
	// PrintMetaMap(remoteIndex)
	for filename, remoteMeta := range remoteIndex {
		if localIndex[filename].GetVersion() < remoteMeta.GetVersion() {
			localIndex[filename] = &FileMetaData{
				Filename:      remoteMeta.GetFilename(),
				Version:       remoteMeta.GetVersion(),
				BlockHashList: remoteMeta.GetBlockHashList(),
			}
			if remoteMeta.GetBlockHashList()[0] == EMPTYFILE_HASHVALUE {
				//Emtpy File
				file, err := os.Create(ConcatPath(client.BaseDir, filename))
				if err != nil {
					localIndex[filename].Version = 0
					return err
				}
				return file.Close()
			} else if remoteMeta.GetBlockHashList()[0] == TOMBSTONE_HASHVALUE {
				//Deleted File
				if err := os.RemoveAll(ConcatPath(client.BaseDir, filename)); err != nil {
					localIndex[filename].Version = 0
					return err
				}
				return nil
			}
			//Normal File
			if err := Download(client, remoteMeta); err != nil {
				localIndex[filename].Version = 0
				return err
			}
		}
	}
	return nil
}

func UploadBlock(client RPCClient, block *Block, blockStoreAddr string) error {
	var succ bool
	if err := client.PutBlock(block, blockStoreAddr, &succ); err != nil {
		return err
	}
	if succ {
		return nil
	} else {
		return errors.New("Upload Block Error")
	}

}

func UploadIfModified(client RPCClient, localMeta *FileMetaData) error {
	hashList, blockList, err := GetChunkedFile(ConcatPath(client.BaseDir, localMeta.Filename), client.BlockSize)
	if err != nil {
		return err
	}
	if len(hashList) == 0 {
		// Empty File
		if len(localMeta.BlockHashList) == 0 || localMeta.BlockHashList[0] != EMPTYFILE_HASHVALUE {
			//Modified
			localMeta.BlockHashList = []string{EMPTYFILE_HASHVALUE}
		} else {
			return nil
		}
	} else {
		//Normal File
		if reflect.DeepEqual(localMeta.GetBlockHashList(), hashList) {
			return nil
		}
		//Modified
		hashAddrMap := make(map[string]string)
		if err := client.GetHashAddrMap(hashList, &hashAddrMap); err != nil {
			return err
		}
		blockStoreMap := make(map[string][]string)
		for hash, addr := range hashAddrMap {
			blockStoreMap[addr] = append(blockStoreMap[addr], hash)
		}

		existHashSet := make(map[string]bool)
		for addr, hashes := range blockStoreMap {
			var existHashes []string
			if err := client.HasBlocks(hashes, addr, &existHashes); err != nil {
				return err
			}
			for _, i := range existHashes {
				existHashSet[i] = true
			}
		}
		for i := 0; i < len(hashList); i++ {
			if !existHashSet[hashList[i]] {
				if err := UploadBlock(client, blockList[i], hashAddrMap[hashList[i]]); err != nil {
					return err
				}
			}
		}
		localMeta.BlockHashList = hashList
	}
	var latestVersion int32
	localMeta.Version += 1
	if err := client.UpdateFile(localMeta, &latestVersion); err != nil {
		localMeta.Version -= 1
		return err
	}
	return nil
}

func PushToServer(client RPCClient, localIndex map[string]*FileMetaData) error {
	remoteIndex := make(map[string]*FileMetaData)
	exist := make(map[string]bool)

	//Create and Modify Files
	err := filepath.Walk(client.BaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		exist[info.Name()] = true
		if info.Name() == DEFAULT_META_FILENAME {
			return nil
		}
		localMeta, exist := localIndex[info.Name()]
		if !exist {
			//Local Create
			if err != nil {
				return err
			}
			localMeta = &FileMetaData{
				Filename: info.Name(),
				Version:  0,
			}
			localIndex[info.Name()] = localMeta
		}
		if err := client.GetFileInfoMap(&remoteIndex); err != nil {
			return err
		}
		if localMeta.GetVersion() < remoteIndex[info.Name()].GetVersion() {
			return OldVerError
		}
		if err := UploadIfModified(client, localMeta); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	//Delete Files
	for fname, localMeta := range localIndex {
		if !exist[fname] && localMeta.BlockHashList[0] != TOMBSTONE_HASHVALUE {
			if err := client.GetFileInfoMap(&remoteIndex); err != nil {
				return err
			}
			if localMeta.GetVersion() < remoteIndex[fname].GetVersion() {
				return OldVerError
			} else {
				deleteMeta := &FileMetaData{
					Filename:      localMeta.Filename,
					Version:       localMeta.Version + 1,
					BlockHashList: []string{TOMBSTONE_HASHVALUE},
				}
				var latestVersion int32
				if err := client.UpdateFile(deleteMeta, &latestVersion); err != nil {
					return err
				}
				localMeta.BlockHashList = []string{TOMBSTONE_HASHVALUE}
				localMeta.Version = latestVersion
			}
		}
	}
	// client.GetFileInfoMap(&remoteIndex)
	// log.Println("Remote after Sync: ")
	// PrintMetaMap(remoteIndex)
	return nil
}

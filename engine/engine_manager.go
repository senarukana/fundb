package engine

import (
	"os"

	abstract "github.com/senarukana/fundb/engine/interface"
	"github.com/senarukana/fundb/engine/leveldb"
)

var (
	defaultDirPerm os.FileMode = 0755
)

type engineNewFunc func() abstract.StoreEngine

var engineImpls = make(map[string]engineNewFunc)

func register(name string, engineInit engineNewFunc) {
	engineImpls[name] = engineInit
}

type EngineManager struct {
	abstract.StoreEngine
	engineName string
	dataPath   string
}

func NewEngineManager(engineName, dataPath string) (*EngineManager, error) {
	if engineNew, ok := engineImpls[engineName]; ok {
		if _, err := os.Stat(dataPath); err != nil && !os.IsExist(err) {
			if err = os.Mkdir(dataPath, defaultDirPerm); err != nil {
				return nil, err
			}
		}
		eng := engineNew()
		if err := eng.Init(dataPath); err != nil {
			return nil, err
		}
		engine := &EngineManager{
			StoreEngine: eng,
			engineName:  engineName,
			dataPath:    dataPath,
		}
		return engine, nil
	} else {
		return nil, ErrUnknownDBEngineName
	}
}

func (engine *EngineManager) EngineName() string {
	return engine.engineName
}

func (engine *EngineManager) DataPath() string {
	return engine.dataPath
}

func (engine *EngineManager) Close() error {
	return engine.Close()
}

func init() {
	register("leveldb", leveldb.NewLevelDBEngine)
}

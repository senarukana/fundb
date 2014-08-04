package engine

import (
	"os"
)

var (
	defaultDirPerm os.FileMode = 0755
)

type engineNewFunc func() storeEngine

var engineImpls = make(map[string]engineNewFunc)

func register(name string, engineInit engineNewFunc) {
	engineImpls[name] = engineInit
}

type EngineManager struct {
	storeEngine
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
			storeEngine: eng,
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
	register("leveldb", NewLevelDBEngine)
}

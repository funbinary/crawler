package sqlstorage

import (
	"encoding/json"
	"github.com/funbinary/crawler/engine"
	"github.com/funbinary/crawler/mysqldb"
	"github.com/funbinary/crawler/storage"
	"go.uber.org/zap"
)

// 实现 Storage 接口的实现

type MySQLStorage struct {
	dataDocker  []*storage.DataCell //分批输出结果缓存
	columnNames []mysqldb.Field     // 标题字段
	db          mysqldb.DBer
	Table       map[string]struct{}
	options     // 选项
}

func New(opts ...Option) (*MySQLStorage, error) {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	s := &MySQLStorage{}
	s.options = options
	s.Table = make(map[string]struct{})
	var err error
	s.db, err = mysqldb.New(
		mysqldb.WithConnUrl(s.sqlUrl),
		mysqldb.WithLogger(s.logger),
	)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *MySQLStorage) Save(dataCells ...*storage.DataCell) error {
	// 循环遍历要存储的 DataCell，并判断当前 DataCell 对应的数据库表是否已经被创建。
	for _, cell := range dataCells {
		name := cell.GetTableName()
		if _, ok := s.Table[name]; !ok {
			// 创建表
			columnNames := getFields(cell)
			err := s.db.CreateTable(mysqldb.TableData{
				TableName:   name,
				ColumnNames: columnNames,
				AutoKey:     true,
			})
			if err != nil {
				s.logger.Error("create table falied", zap.Error(err))
			}
			s.Table[name] = struct{}{}
		}
		// 如果当前的数据小于 s.BatchCount，则将数据放入到缓存中直接返回（使用缓冲区批量插入数据库可以提高程序的性能）。
		if len(s.dataDocker) >= s.BatchCount {
			if err := s.Flush(); err != nil {
				s.logger.Error("insert data failed", zap.Error(err))
			}
		}
		// 如果缓冲区已经满了，则调用 SqlStore.Flush() 方法批量插入数据。
		s.dataDocker = append(s.dataDocker, cell)
	}
	return nil
}

// getFields 用于获取当前数据的表字段与字段类型，这是从采集规则节点的 ItemFields 数组中获得的。
// 为什么不直接用 DataCell 中 Data 对应的哈希表中的 Key 生成字段名呢？
// 这一方面是因为它的速度太慢，另外一方面是因为 Go 中的哈希表在遍历时的顺序是随机的，而生成的字段列表需要顺序固定。
func getFields(cell *storage.DataCell) []mysqldb.Field {
	taskName := cell.Data["Task"].(string)
	ruleName := cell.Data["Rule"].(string)
	fields := engine.GetFields(taskName, ruleName)

	var columnNames []mysqldb.Field
	for _, field := range fields {
		columnNames = append(columnNames, mysqldb.Field{
			Title: field,
			Type:  "MEDIUMTEXT",
		})
	}
	columnNames = append(columnNames,
		mysqldb.Field{Title: "Url", Type: "VARCHAR(255)"},
		mysqldb.Field{Title: "Time", Type: "VARCHAR(255)"},
	)
	return columnNames
}

// Flush 核心是遍历缓冲区，解析每一个 DataCell 中的数据，将扩展后的字段值批量放入 args 参数中，
// 并调用底层 DBer.Insert 方法批量插入数据
func (s *MySQLStorage) Flush() (err error) {
	if len(s.dataDocker) == 0 {
		return nil
	}
	defer func() {
		s.dataDocker = nil
	}()
	args := make([]interface{}, 0)
	for _, datacell := range s.dataDocker {
		ruleName := datacell.Data["Rule"].(string)
		taskName := datacell.Data["Task"].(string)
		fields := engine.GetFields(taskName, ruleName)
		data := datacell.Data["Data"].(map[string]interface{})
		value := []string{}
		for _, field := range fields {
			v := data[field]
			switch v.(type) {
			case nil:
				value = append(value, "")
			case string:
				value = append(value, v.(string))
			default:
				j, err := json.Marshal(v)
				if err != nil {
					value = append(value, "")
				} else {
					value = append(value, string(j))
				}
			}
		}
		value = append(value, datacell.Data["Url"].(string), datacell.Data["Time"].(string))
		for _, v := range value {
			args = append(args, v)
		}
	}

	return s.db.Insert(mysqldb.TableData{
		TableName:   s.dataDocker[0].GetTableName(),
		ColumnNames: getFields(s.dataDocker[0]),
		Args:        args,
		DataCount:   len(s.dataDocker),
	})
}

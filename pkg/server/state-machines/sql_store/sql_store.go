package sqlstore

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/expr/builtins"
	_ "github.com/lytics/qlbridge/qlbdriver"
	"github.com/lytics/qlbridge/schema"
	"go.uber.org/zap"
)

const SQLSourceID = "_expodb_local_"

var (
	// Ensure our SQLDBSource implements schema.Source
	_ schema.Source = (*SQLDBSource)(nil)

	// Ensure our SQLDBConn implements variety of Connection interfaces.
	_ schema.Conn           = (*SQLDBConn)(nil)
	_ schema.ConnScanner    = (*SQLDBConn)(nil)
	_ schema.ConnUpsert     = (*SQLDBConn)(nil)
	_ schema.ConnPatchWhere = (*SQLDBConn)(nil)
	_ schema.ConnDeletion   = (*SQLDBConn)(nil)
	_ schema.ConnSeeker     = (*SQLDBConn)(nil)
)

// SQL DataSource, implements qlbridge schema DataSource, SourceConn, Scanner
//   to allow csv files to be full featured databases.
//   - very, very naive scanner, forward only single pass
//   - can open a file with .Open()
//   - assumes comma delimited
//   - does not implement write operations
//
// TODO MAKE thread-safe
type SQLDBSource struct {
	mu     *sync.RWMutex
	logger *zap.Logger

	// table -> rows -> cols[col_vals]
	stateValue map[string]map[string]map[string]string
	// table -> rows -> cols[col_vals]
	stateValArr map[string][]map[string]string
}

/*

	sqlstore.Init(logger, currentState)
	db, err := sql.Open("qlbridge", sqlstore.SQLSourceID )
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
*/
func Init(logger *zap.Logger, currentState map[string]map[string]map[string]string) error {

	arrMap := map[string][]map[string]string{}
	for tabName, tab := range currentState {
		arr := []map[string]string{}
		for _, row := range tab {
			arr = append(arr, row)
		}
		arrMap[tabName] = arr
	}

	src := &SQLDBSource{
		mu:          &sync.RWMutex{},
		logger:      logger,
		stateValue:  currentState,
		stateValArr: arrMap,
	}

	// load all of our built-in functions
	// This allows you to do things like use count, i.e. `select count(*) from foo`
	builtins.LoadAllBuiltins()

	// TODO maybe I need to use registry.RegisterByConf?
	return schema.RegisterSourceAsSchema(SQLSourceID, src) // Register this source with qlbridge
}

// Init provides opportunity for those sources that require/ no configuration and
// introspect schema from their environment time to load pre-schema discovery
func (s *SQLDBSource) Init() { /* nothing implemented yet */ }

// Setup optional interface for getting the Schema injected during creation/startup.
// Since the Source is a singleton, stateful manager,  it has a startup/shutdown process.
func (s *SQLDBSource) Setup(*schema.Schema) error { return nil }

// Close this source, ensure connections, underlying resources are closed.
func (s *SQLDBSource) Close() error { return nil }

// Open create a connection (not thread safe) to this source.
func (s *SQLDBSource) Open(table string) (schema.Conn, error) {
	return &SQLDBConn{
		mu:      s.mu,
		table:   table,
		src:     s,
		currPos: 0,
	}, nil
}

// Tables is a list of table names provided by this source.
func (s *SQLDBSource) Tables() []string { panic("not implemented") }

// Table get table schema for given table name.
func (s *SQLDBSource) Table(table string) (*schema.Table, error) { panic("not implemented") }

type SQLDBConn struct {
	mu      *sync.RWMutex
	table   string
	src     *SQLDBSource
	currPos int
}

func (s *SQLDBConn) Close() error {
	return nil
}

// Next returns the next message.  If none remain, returns nil.
func (s *SQLDBConn) Next() schema.Message {
	tab := s.src.stateValArr[s.table]
	for {
		if s.currPos >= len(tab) {
			return nil
		}
		//vals := make([]driver.Value, len(m.cols))
		//u.Infof("expecting %d cols", len(m.cols))
		readCols := make([]interface{}, len(m.cols))
		writeCols := make([]driver.Value, len(m.cols))
		for i := range writeCols {
			readCols[i] = &writeCols[i]
		}
		//cols, _ := m.rows.Columns()
		//u.Debugf("sqlite result cols provides %v but expecting %d", cols, len(m.cols))

		m.err = m.rows.Scan(readCols...)
		if m.err != nil {
			u.Warnf("err=%v", m.err)
			return nil
		}
		//u.Debugf("read vals: %#v", writeCols)

		// This seems pretty gross, isn't there a better way to do this?
		for i, col := range writeCols {
			//u.Debugf("%d %s  %T %v", i, m.cols[i], col, col)
			switch val := col.(type) {
			case []uint8:
				writeCols[i] = driver.Value(string(val))
			}
		}
		msg := datasource.NewSqlDriverMessageMap(m.ct, writeCols, m.colidx)

		m.ct++

		//u.Infof("return item btreeP:%p itemP:%p cursorP:%p  %v %v", m, item, m.cursor, msg.Id(), msg.Values())
		//u.Debugf("return? %T  %v", item, item.(*DriverItem).SqlDriverMessageMap)
		return msg
	}
}

// ConnUpsert Mutation interface for Put
//  - assumes datasource understands key(s?)
func (s *SQLDBConn) Put(ctx context.Context, key schema.Key, value interface{}) (schema.Key, error) {
	panic("not implemented")
}

// ConnUpsert Mutation interface for PutMulti
//  - assumes datasource understands key(s?)
func (s *SQLDBConn) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	panic("not implemented")
}

// ConnPatchWhere pass through where expression to underlying datasource
// Used for update statements WHERE x = y
func (s *SQLDBConn) PatchWhere(ctx context.Context, where expr.Node, patch interface{}) (int64, error) {
	panic("not implemented")
}

// Delete using this key
func (s *SQLDBConn) Delete(driver.Value) (int, error) { panic("not implemented") }

// Delete with given expression
func (s *SQLDBConn) DeleteExpression(p interface{} /* plan.Delete */, n expr.Node) (int, error) {
	panic("not implemented")
}

// ConnSeeker is a conn that is Key-Value store, allows relational
// implementation to be faster for Seeking row values instead of scanning
func (s *SQLDBConn) Get(key driver.Value) (schema.Message, error) { panic("not implemented") }

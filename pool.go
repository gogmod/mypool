package mypool

import (
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_DB_CONNECTIONS = 100

type DBPool interface {
	InitPool(database string, constr string, initConnNum int) error
	GetConnection() Connection
	ReleaseConnection(connection Connection) bool
	DestroyPool()
}

type DefaultDBPool struct {
	dbname    string
	dbconnstr string
	dbconnNum int
	freeConns []Connection
	curConnID int
	lock      sync.Mutex
}

func NewDefaultDBPool() DBPool {
	pool := &DefaultDBPool{}
	pool.freeConns = make([]Connection, 0)
	return pool
}

func (pool *DefaultDBPool) InitPool(database string, connstr string, initConnNum int) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	pool.dbname = database
	pool.dbconnstr = connstr
	if initConnNum <= 0 {
		initConnNum = 5
	}
	pool.dbconnNum = 0
	for i := 0; i < initConnNum; i++ {
		_, err := pool.createConnection()
		if err != nil {
			return err
		}
	}
	return nil
}

func (pool *DefaultDBPool) getNextConnID() int {
	pool.curConnID++
	return pool.curConnID
}

//createConnection...
func (pool *DefaultDBPool) createConnection() (Connection, error) {
	db, err := sql.Open("mysql", pool.dbconnstr)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	conn := NewDBConnection(db)
	conn.SetID(pool.getNextConnID())
	conn.Used(false)
	pool.dbconnNum++
	pool.freeConns = append(pool.freeConns, conn)
	return conn, nil
}

func (pool *DefaultDBPool) GetConnection() Connection {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, tmp := range pool.freeConns {
		if false == tmp.IsUsed() {
			tmp.Used(true)
			return tmp
		}
	}
	if pool.dbconnNum < MAX_DB_CONNECTIONS {
		conn, err := pool.createConnection()
		if err != nil {
			return nil
		}
		conn.Used(true)
		return conn
	}
	return nil
}

func (pool *DefaultDBPool) ReleaseConnection(conn Connection) bool {
	if conn == nil {
		return false
	}
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, tmp := range pool.freeConns {
		if tmp.GetID() == conn.GetID() {
			tmp.Used(false)
			return true
		}
	}
	return false
}

func (pool *DefaultDBPool) DestroyPool() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, tmp := range pool.freeConns {
		tmp.Close()
	}
}

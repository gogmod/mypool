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
	dbconnNum string
	freeConns []Connection
	curConnID int
	lock      sync.Mutex
}

func NewDefaultDBPool() DBPool {
	pool := &DefaultDBPool{}
	pool.freeConns = make([]Connection, 0)
	return pool
}

func (this *DefaultDBPool) InitPool(database string, connstr string, initConnNum int) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.dbname = database
	this.dbconnstr = connstr
	if initConnNum <= 0 {
		initConnNum = 5
	}
	this.dbconnNum = 0
	for i := 0; i < initConnNum; i++ {
		_, err := this.createConnection()
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *DefaultDBPool) getNextConnId() int {
	this.curConnID++
	return this.curConnID
}

func (this *DefaultDBPool) createConnection() (Connection, error) {
	db, err := sql.Open("mysql", this.dbconnstr)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	conn := NewDBConnection(db)
	conn.SetID(this.getNextConnID())
	conn.Used(false)
	this.dbconnNum++
	this.freeConns = append(this.freeConns, conn)
	return conn, nil
}

func (this *DefaultDBPool) GetConnection() Connection {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, tmp := range this.freeConns {
		if false == tmp.IsUsed() {
			tmp.Used(true)
			return tmp
		}
	}
	if this.dbconnNum < MAX_DB_CONNECTIONS {
		conn, err := this.createConnection()
		if err != nil {
			return nil
		}
		conn.Used(true)
		return conn
	}
	return nil
}

func (this *DefaultDBPool) ReleaseConnection(conn Connection) bool {
	if conn == nil {
		return false
	}
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, tmp := range this.freeConns {
		if tmp.GetID() == conn.GetID() {
			tmp.Used(false)
			return true
		}
	}
	return false
}

func (this *DefaultDBPool) DestroyPool() {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, tmp := range this.freeConns {
		tmp.Close()
	}
}

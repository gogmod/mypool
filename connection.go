package mypool

import (
	"database/sql"
	"sync"
)

type Connection interface {
	GetID() int
	SetID(id int)
	Close() error
	Create(sql string) error
	Read(sql string) (*sql.Rows, error)
	Update(sql string) error
	Delete(sql string) error
	IsUsed() bool
	Used(b bool)
}

type DBConnection struct {
	*sql.DB
	id int
	sync.RWMutex
	used bool
}

func NewDBConnection(db *sql.DB) *DBConnection {
	conn := &DBConnection{}
	conn.DB = db
	return conn
}

func (conn *DBConnection) GetID() int {
	return conn.id
}

func (conn *DBConnection) SetID(id int) {
	conn.id = id
}

func (conn *DBConnection) Close() error {
	return conn.DB.Close()
}

func (conn *DBConnection) Create(sql string) error {
	result, err := conn.DB.Query(sql)
	if err != nil {
		return err
	} else {
		defer result.Close()
		return err
	}
}

func (conn *DBConnection) Read(sql string) (*sql.Rows, error) {
	result, err := conn.DB.Query(sql)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *DBConnection) Update(sql string) error {
	result, err := conn.DB.Query(sql)
	if err != nil {
		return err
	} else {
		defer result.Close()
		return err
	}
}

func (conn *DBConnection) Delete(sql string) error {
	result, err := conn.DB.Query(sql)
	if err != nil {
		return err
	} else {
		defer result.Close()
		return err
	}
}

func (conn *DBConnection) IsUsed() bool {
	conn.RWMutex.RLock()
	defer conn.RWMutex.RUnlock()
	return conn.used
}

func (conn *DBConnection) Used(b bool) {
	conn.RWMutex.Lock()
	defer conn.RWMutex.Unlock()
	conn.used = b
}

package sqlx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strconv"
	"testing"
	"time"

	"github.com/bingoohuang/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// person 结构体，对应到person表字段.
type person struct {
	ID  string
	Age int
}

// personDao 定义对person表操作的所有方法.
type personDao struct {
	CreateTable func()                         `sql:"create table person(id varchar(100), age int)"`
	Add         func(person)                   `sql:"insert into person(id, age) values(:id, :age)"`
	AddAll      func(...person)                `sql:"insert into person(id, age) values(:id, :age)"`
	AddAll2     func(...person) uint64         `sql:"insert into person(id, age) values(:id, :age)"`
	AddAll3     func(...person) int            `sql:"insert into person(id, age) values(:id, :age)"`
	AddAll4     func(...person) (int, error)   `sql:"insert into person(id, age) values(:id, :age)"`
	Find        func(id string) person         `sql:"select id, age from person where id=:1"`
	Find2       func(id string) *person        `sql:"select id, age from person where id=:1"`
	ListAll     func() []person                `sql:"select id, age from person"`
	ListByID    func(string) []person          `sql:"select id, age from person where id=:1"`
	Delete      func(string) int               `sql:"delete from person where id=:1"`
	GetAge      func(string) struct{ Age int } `sql:"select age from person where id=:1"`
}

func TestDao(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao{}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao))

	// 建表
	dao.CreateTable()
	// 插入
	dao.Add(person{"100", 100})
	// 查找
	that.Equal(person{"100", 100}, dao.Find("100"))
	that.Equal(person{"100", 100}, *dao.Find2("100"))
	// 刪除
	that.Equal(1, dao.Delete("100"))
	// 再找，找不到，返回零值
	that.Zero(dao.Find("100"))
	// 再找，找不到，返回零值
	v := dao.Find2("100")
	that.Nil(v)
	// 插入
	dao.Add(person{"200", 200})
	// 多值插入
	dao.AddAll(person{"300", 300}, person{"400", 400})
	// 列表
	that.Equal([]person{{"200", 200}, {"300", 300}, {"400", 400}}, dao.ListAll())
	// 条件列表
	that.Equal([]person{{"200", 200}}, dao.ListByID("200"))
	// 匿名结构
	that.Equal(struct{ Age int }{Age: 200}, dao.GetAge("200"))

	// 多值插入
	lastInsertID2 := dao.AddAll2(person{"500", 500}, person{"600", 600})
	assert.True(t, lastInsertID2 > 0)

	lastInsertID3 := dao.AddAll3(person{"700", 500}, person{"701", 600})
	assert.True(t, lastInsertID3 > 0)

	lastInsertID4, err := dao.AddAll4(person{"900", 500}, person{"702", 600})
	assert.True(t, lastInsertID4 > 0)
	assert.Nil(t, err)
}

func openDB(t *testing.T) *sql.DB {
	// 创建数据库连接池
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	return db
}

// personDao2 定义对person表操作的所有方法.
type personDao2 struct {
	CreateTable func()          `sql:"create table person(id varchar(100), age int)"`
	AddAll      func(...person) `sql:"insert into person(id, age) values(:id, :age)"`

	GetAgeE func(string) (struct{ Age int }, error) `sql:"select age from person where xid=:1"`
	GetAgeX func(string) person                     `sql:"select age from person where xid=:1"`

	Err error // 添加这个字段，可以用来单独接收error信息

	ListAll func() []person `sql:"select id, age from person order by id"`
}

func TestDaoWithError(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao2{}

	var err error

	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithError(&err)))

	dao.CreateTable()

	that.Nil(dao.Err)
	ageX, err := dao.GetAgeE("200")
	that.Error(err)
	that.Zero(ageX)
	that.Error(dao.Err)

	// 条件列表
	dao.AddAll(person{"200", 200})
	that.Nil(dao.Err) // 验证Err字段是否重置

	that.Zero(dao.GetAgeX("100"))
	that.Error(err)
}

func TestDaoWithContext(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao2{}
	// Pass a context with a timeout to tell a blocking function that it
	// should abandon its work after the timeout elapses.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithCtx(ctx), sqlx.WithLimit(1)))

	dao.CreateTable()

	// 多值插入
	dao.AddAll(person{"300", 300}, person{"400", 400})

	peoples := dao.ListAll()
	that.Len(peoples, 1)
}

func TestDaoWithRowScanInterceptor(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao2{}
	p := person{}
	f := sqlx.RowScanInterceptorFn(func(rowIndex int, v ...interface{}) (bool, error) {
		p = v[0].(person)
		return false, nil
	})
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithRowScanInterceptor(f)))

	dao.CreateTable()

	// 多值插入
	dao.AddAll(person{"300", 300}, person{"400", 400})

	peoples := dao.ListAll()
	that.Len(peoples, 0)
	that.Equal(person{"300", 300}, p)
}

// personDao3 定义对person表操作的所有方法.
type personDao3 struct {
	CreateTable func()
	AddAll      func(...person)
	ListAll     func() []person
	Logger      sqlx.DaoLogger
}

const dotSQL = `
-- name: CreateTable
create table person(id varchar(100), age int);

-- name: AddAll
insert into person(id, age) values(:id, :age);

-- name: ListAll delimiter: /
/select id, age from person order by id//
`

func TestDaoWithDotSQLString(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao3{Logger: &sqlx.DaoLogrus{}}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithSQLStr(dotSQL)))

	dao.CreateTable()

	// 多值插入
	dao.AddAll(person{"300", 300}, person{"400", 400})

	// 列表
	that.Equal([]person{{"300", 300}, {"400", 400}}, dao.ListAll())
}

func TestDaoWithDotSQLFile(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao3{}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithSQLFile(`testdata/d3.sql`)))

	dao.CreateTable()

	// 多值插入
	dao.AddAll(person{"300", 300}, person{"400", 400})

	// 列表
	that.Equal([]person{{"300", 300}, {"400", 400}}, dao.ListAll())
}

// person 结构体，对应到person表字段.
type person4 struct {
	ID  sql.NullString
	Age int
}

// person 结构体，对应到person表字段.
type person5 struct {
	ID    string
	Age   int
	Other string
	Addr  sql.NullString
}

type Addr string

// person 结构体，对应到person表字段.
type person6 struct {
	Addr Addr
}

// personDao4 定义对person表操作的所有方法.
type personDao4 struct {
	CreateTable func()            `sql:"create table person(id varchar(100), age int, addr varchar(10) default 'bj')"`
	AddID       func(int)         `sql:"insert into person(age, addr) values(:1, 'zags')"`
	Add         func(M)           `sql:"insert into person(id, age, addr) values(:id, :age, :addr)"`
	FindByAge1  func(int) person4 `sql:"select id, age, addr from person where age = :1"`
	FindByAge2  func(int) person5 `sqlName:"FindByAge1"`
	FindByAge3  func(int) person6 `sqlName:"FindByAge1"`
}

func TestNullString(t *testing.T) {
	that := assert.New(t)

	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDao4{}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao))

	dao.CreateTable()
	dao.AddID(100)
	p1 := dao.FindByAge1(100)
	that.Equal(person4{ID: sql.NullString{Valid: false}, Age: 100}, p1)

	p2 := dao.FindByAge2(100)
	that.Equal(person5{ID: "", Age: 100, Addr: sql.NullString{String: "zags", Valid: true}}, p2)

	p3 := dao.FindByAge3(100)
	that.Equal(person6{Addr: "zags"}, p3)
}

// personMap 结构体，对应到person表字段.
type personMap struct {
	ID   string
	Age  int
	Addr string
}

const dotSQLMap = `
-- name: CreateTable
create table person(id varchar(100), age int, addr varchar(10));

-- name: Find
select id, age, addr from person where id = :1;

-- name: Add
insert into person(id, age, addr) values(:id, :age, :addr);

-- name: Count
select count(*) from person

-- name: Get2
select id, addr from person where id=:1
`

// personDaoMap 定义对person表操作的所有方法.
type personDaoMap struct {
	CreateTable func()
	Add         func(m M)
	Find        func(id string) personMap
	FindAsMap1  func(id string) map[string]string `sqlName:"Find"`
	FindAsMap2  func(id string) M                 `sqlName:"Find"`

	Count    func() (count int)
	GetAddr  func(id string) (addr string) `sql:"select addr from person where id=:1"`
	GetAddr2 func(id string) (addr Addr)   `sqlName:"GetAddr"`
	Get2     func(id string) (pid, addr string)
	Get3     func(id string) (pid string)                     `sqlName:"Get2"`
	Get4     func(id string) (pid, addr, nos string, noi int) `sqlName:"Get2"`

	Logger sqlx.DaoLogger
	Error  error
}

type M map[string]interface{}

func TestMapArg(t *testing.T) {
	that := assert.New(t)

	logrus.SetLevel(logrus.DebugLevel)
	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDaoMap{Logger: &sqlx.DaoLogrus{}}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithSQLStr(dotSQLMap)))

	dao.CreateTable()
	dao.Add(M{"id": "40685", "age": 500, "addr": "bjca"})
	dao.Add(M{"id": "40686", "age": 600, "addr": nil})

	that.Equal(personMap{ID: "40685", Age: 500, Addr: "bjca"}, dao.Find("40685"))
	that.Equal(personMap{ID: "40686", Age: 600, Addr: ""}, dao.Find("40686"))

	that.Equal(map[string]string{"id": "40685", "age": "500", "addr": "bjca"}, dao.FindAsMap1("40685"))
	that.Equal(M{"id": "40685", "age": int64(500), "addr": "bjca"}, dao.FindAsMap2("40685"))

	that.Equal(2, dao.Count())
	that.Equal("bjca", dao.GetAddr("40685"))
	that.Equal(Addr("bjca"), dao.GetAddr2("40685"))

	id, addr := dao.Get2("40685")
	that.Equal("40685", id)
	that.Equal("bjca", addr)
	that.Equal("40685", dao.Get3("40685"))

	{
		id, addr, other, otherInt := dao.Get4("40685")
		that.Equal("40685", id)
		that.Equal("bjca", addr)
		that.Empty(other)
		that.Zero(otherInt)
	}

	{
		id, addr, other, otherInt := dao.Get4("50685")
		that.Empty(id)
		that.Empty(addr)
		that.Empty(other)
		that.Zero(otherInt)
		that.Equal(sql.ErrNoRows, dao.Error)
	}
}

const dotSQLDynamic = `
-- name: CreateTable
create table person(id varchar(100), age int, addr varchar(10));

-- name: Add
insert into person(id, age, addr) values(:id, :age, :addr);

-- name: GetAddr
select addr from person where id = :1 /* if _2 > 0 */ and age = :2 /* end */;

-- name: GetAddr2
select addr from person where id = :1
-- if _2 > 0 
and age = :2 
-- end
;

-- name: GetAddrStruct
select addr from person where id = :id /* if age > 0 */ and age = :age /* end */;

-- name: GetAddrStruct2
select addr from person where id = :id
-- if age > 0 
and age = :age
-- end
;
`

// personDaoDynamic 定义对person表操作的所有方法.
type personDaoDynamic struct {
	CreateTable func()
	Add         func(m M)
	GetAddr     func(id string, age int) (addr string)
	GetAddr2    func(id string, age int) (addr string)

	GetAddrStruct  func(p personMap) (addr string)
	GetAddrStruct2 func(p personMap) (addr string)
	GetAddrStruct3 func(p personMap) (addresses []string) `sqlName:"GetAddrStruct2"`

	Logger sqlx.DaoLogger
	Error  error
}

func TestMapDynamic(t *testing.T) {
	that := assert.New(t)

	logrus.SetLevel(logrus.DebugLevel)
	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personDaoDynamic{Logger: &sqlx.DaoLogrus{}}
	sqlx.DB = openDB(t)
	that.Nil(sqlx.CreateDao(dao, sqlx.WithSQLStr(dotSQLDynamic)))

	dao.CreateTable()
	dao.Add(M{"id": "40685", "age": 500, "addr": "bjca"})
	dao.Add(M{"id": "40685", "age": 600, "addr": "acjb"})

	that.Equal("bjca", dao.GetAddr("40685", 0))
	that.Equal("acjb", dao.GetAddr("40685", 600))

	that.Equal("bjca", dao.GetAddrStruct(personMap{ID: "40685"}))
	that.Equal("acjb", dao.GetAddrStruct(personMap{ID: "40685", Age: 600}))

	that.Equal("bjca", dao.GetAddr2("40685", 0))
	that.Equal("acjb", dao.GetAddr2("40685", 600))

	that.Equal("bjca", dao.GetAddrStruct2(personMap{ID: "40685"}))
	that.Equal("acjb", dao.GetAddrStruct2(personMap{ID: "40685", Age: 600}))

	that.Equal([]string{"bjca", "acjb"}, dao.GetAddrStruct3(personMap{ID: "40685"}))
	that.Equal([]string{"acjb"}, dao.GetAddrStruct3(personMap{ID: "40685", Age: 600}))
}

const dotSQLLastInsertID = `
-- name: CreateTable
create table person(id INTEGER PRIMARY KEY AUTOINCREMENT, age int, addr varchar(10));

-- name: Add
insert into person( age, addr) values( :age, :addr);

-- name: Update
update person set age = :age, addr = :addr where id = :id;
;
`

type personLastInsertID struct {
	CreateTable func()
	Add         func(m M) (lastInsertID int)
	Update      func(m M) (effectedRows int)
	Update2     func(m M) (effectedRows, lastInsertID int) `sqlName:"Update"`
	Logger      sqlx.DaoLogger
	Error       error
}

func TestLastInsertID(t *testing.T) {
	that := assert.New(t)

	logrus.SetLevel(logrus.DebugLevel)
	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personLastInsertID{Logger: &sqlx.DaoLogrus{}}
	that.Nil(sqlx.CreateDao(dao, sqlx.WithDB(openDB(t)), sqlx.WithSQLStr(dotSQLLastInsertID)))

	dao.CreateTable()
	that.True(dao.Add(M{"age": 500, "addr": "bjca"}) > 0)
	lastInsertID := dao.Add(M{"age": 600, "addr": "acjb"})
	that.True(lastInsertID > 0)
	that.Equal(1, dao.Update(M{"id": lastInsertID, "age": 601, "addr": "xx"}))
	effectedRows, insertID := dao.Update2(M{"id": lastInsertID, "age": 602, "addr": "yy"})
	that.Equal(1, effectedRows)
	that.Equal(lastInsertID, insertID)
}

type MyTime time.Time

// Marshaler is the interface implemented by types that
// can marshal themselves into valid JSON.
func (m MyTime) MarshalJSON() ([]byte, error) {
	s := (time.Time(m)).Format("2006-01-02 15:04:05")
	return []byte(strconv.Quote(s)), nil
}

// Value - Implementation of valuer for database/sql.
func (m MyTime) Value() (driver.Value, error) {
	return driver.Value(time.Time(m)), nil
}

const dotSQLTime = `
-- name: CreateTable
create table person(id INTEGER PRIMARY KEY AUTOINCREMENT, age int, addr varchar(10), birthday timestamp);

-- name: Add
insert into person( age, addr, birthday ) values( :age, :addr, :birthday);

-- name: Find
select id, age, addr, birthday from person where id = :1;

-- name: Delete
delete from person  where id = ':';;
`

type personTime struct {
	ID       uint64
	Age      int
	Addr     string
	Birthday MyTime
}
type personTimeDao struct {
	CreateTable func()
	Add         func(m personTime) (lastInsertID uint64)
	Find        func(uint64) (personTime, error)
	Delete      func(uint64) (effectedRows int)

	Logger sqlx.DaoLogger
	Error  error
}

func TestTime(t *testing.T) {
	that := assert.New(t)

	logrus.SetLevel(logrus.DebugLevel)
	// 生成DAO，自动创建dao结构体中的函数字段
	dao := &personTimeDao{Logger: &sqlx.DaoLogrus{}}
	that.Nil(sqlx.CreateDao(dao, sqlx.WithDB(openDB(t)), sqlx.WithSQLStr(dotSQLTime)))

	now, _ := time.Parse("2006-01-02 15:04:05", "2020-03-31 17:17:40")

	dao.CreateTable()

	p := personTime{
		Age:      10,
		Addr:     "aa",
		Birthday: MyTime(now),
	}

	lastInsertID := dao.Add(p)
	p.ID = lastInsertID

	p2, err := dao.Find(lastInsertID)
	that.Equal(p, p2)
	that.Nil(err)

	effectedRows := dao.Delete(lastInsertID)
	that.Equal(1, effectedRows)
}

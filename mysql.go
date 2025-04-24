package sqlx

import (
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/spf13/viper"
)

// MySQLMore MySQL增强器.
type MySQLMore struct {
	dbDriver string
}

// NewMySQLMore 新建MySQL增强器.
func NewMySQLMore(dbDriver string) *MySQLMore {
	return &MySQLMore{dbDriver: dbDriver}
}

// Matches 是否匹配当前实现.
func (m *MySQLMore) Matches() bool { return m.dbDriver == "mysql" }

// EnhanceURI 增强URI.
func (m *MySQLMore) EnhanceURI(dbURI string) string {
	// user:pass@tcp(192.168.136.90:3307)/sdb?charset=utf8mb4&parseTime=true&loc=Local
	// refer
	// 1. https://github.com/go-sql-driver/mysql
	// 2. https://gorm.io/docs/connecting_to_the_database.html
	// 3. https://stackoverflow.com/questions/40527808/setting-tcp-timeout-for-sql-connection-in-go
	enhanced := attachParameter(dbURI, "charset", "utf8mb4")
	enhanced += attachParameter(dbURI, "parseTime", "true")
	enhanced += attachParameter(dbURI, "loc", "Local")
	enhanced += attachParameter(dbURI, "timeout", "10s")
	enhanced += attachParameter(dbURI, "writeTimeout", "10s")
	enhanced += attachParameter(dbURI, "readTimeout", "10s")

	if enhanced != "" && !strings.Contains(dbURI, "?") {
		enhanced = "?" + enhanced[1:]
	}

	return dbURI + enhanced
}

func attachParameter(dbURI, key, value string) string {
	if strings.Contains(dbURI, key+"=") {
		return ""
	}

	return "&" + key + "=" + value
}

// EnhanceGormDB 增强GormDB.
func (m *MySQLMore) EnhanceGormDB(db *gorm.DB) *gorm.DB {
	return db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
}

// ViperMySQLBindAddress bind client address by viper flag bindAddress.
func ViperMySQLBindAddress() error {
	bindAddress := viper.GetString("bindAddress")
	if bindAddress == "" {
		return nil
	}

	const netKey = "mysqlNet"

	viper.SetDefault(netKey, "tcp")
	mysqlNet := viper.GetString(netKey)

	return MySQLBindAddress(mysqlNet, bindAddress, &net.Dialer{
		Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, // nolint:gomnd
	})
}

// MySQLBindAddress bind client address.
func MySQLBindAddress(mysqlNet, bindAddress string, defaultDialer *net.Dialer) error {
	// https://stackoverflow.com/questions/33768557/how-to-bind-an-http-client-in-go-to-an-ip-address
	ip, err := ResolveIP(bindAddress)
	if err != nil {
		return err
	}

	nd := net.Dialer{}

	if defaultDialer != nil {
		nd = *defaultDialer
	} else {
		nd.Timeout = 30 * time.Second // nolint:gomnd
		nd.KeepAlive = nd.Timeout
	}

	nd.LocalAddr = &net.TCPAddr{IP: ip}

	// https://gist.github.com/jayjanssen/8e74bc4c5bdefc880ffd
	f := func(addr string) (net.Conn, error) { return nd.Dial(`tcp`, addr) }
	mysql.RegisterDial(mysqlNet, f)

	return nil
}

// ResolveIP resolves the address to IP.
func ResolveIP(address string) (net.IP, error) {
	if IsIP(address) {
		return net.ParseIP(address), nil
	}

	ipAddr, err := net.ResolveIPAddr("ip", address)
	if err != nil {
		return nil, err
	}

	return ipAddr.IP, nil
}

// IsIP 判断 host 字符串表达式是不是IP(v4/v6)的格式
func IsIP(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil
}

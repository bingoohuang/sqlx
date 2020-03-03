package sqlmore

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/bingoohuang/gou/str"
	shellwords "github.com/mattn/go-shellwords"
	"github.com/spf13/pflag"
)

// CompatibleMySQLDs make mysql datasource be compatible with raw， mysql or gossh host format.
func CompatibleMySQLDs(s string) string {
	// user:pass@tcp(localhost:3306)/db?charset=utf8mb4&parseTime=true&loc=Local
	if strings.Contains(s, "@tcp") {
		return s
	}

	// MYSQL_PWD=8BE4 mysql -h 127.0.0.1 -P 9633 -u root
	// -u, --user=name     User for login if not current user.
	if strings.Contains(s, " -u") || strings.Contains(s, " --user") {
		return compatibleMySQLClientCmd(s)
	}

	// 127.0.0.1:9633 root/8BE4 [db=db]
	if strings.Contains(s, ":") || strings.Contains(s, "/") {
		if v, ok := compatibleGoSSHHost(s); ok {
			return v
		}
	}

	return s
}

func compatibleGoSSHHost(s string) (string, bool) {
	// 127.0.0.1:9633 root/8BE4 [db=db]
	fields := str.FieldsX(s, "", "", 3)
	if len(fields) < 2 { // nolint gomnd
		return "", false
	}

	host, port := parseHostPort(fields[0], "3306")
	user, password := str.Split2(fields[1], "/", true, true)
	props := parseProps(fields)
	db := ""

	if v, ok := props["db"]; ok {
		db = v
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		user, password, host, port, db), true
}

func parseProps(fields []string) map[string]string {
	props := make(map[string]string)

	for i := 2; i < len(fields); i++ {
		k, v := str.Split2(fields[i], "=", true, true)
		props[k] = v
	}

	return props
}

func parseHostPort(addr, defaultPort string) (string, string) {
	pos := strings.Index(addr, ":")
	if pos < 0 {
		return addr, defaultPort
	}

	return addr[0:pos], addr[pos+1:]
}

func compatibleMySQLClientCmd(s string) string {
	if pos := strings.Index(s, "MYSQL_PWD="); pos >= 0 {
		s = s[0:pos] + "--" + s[pos:]
	}

	if pos := strings.Index(s, "mysql "); pos >= 0 {
		s = s[0:pos] + "--" + s[pos:]
	}

	pf := pflag.NewFlagSet("ds", pflag.ExitOnError)

	pf.BoolP("mysql", "", false, "mysql command")
	pf.StringP("MYSQL_PWD", "", "", "MYSQL_PWD env password")
	pf.StringP("database", "D", "", "Database to use")
	pf.StringP("host", "h", "", "Connect to host")
	pf.IntP("port", "P", 3306, "Port number to use")
	pf.StringP("user", "u", "", "User for login if not current user")
	pf.StringP("password", "p", "", "Password to use when connecting to serve")

	p := shellwords.NewParser()
	p.ParseEnv = true
	args, err := p.Parse(s)

	if err != nil {
		logrus.Fatalf("Fail to parse ds %s error %v", s, err)
	}

	if err := pf.Parse(args); err != nil {
		return s
	}

	host, _ := pf.GetString("host")
	port, _ := pf.GetInt("port")
	user, _ := pf.GetString("user")
	db, _ := pf.GetString("database")
	password, _ := pf.GetString("password")

	if password == "" {
		password, _ = pf.GetString("MYSQL_PWD")
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		user, password, host, port, db)
}

package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

const (
	connectionString = "php-crud-api:php-crud-api@unix(/var/run/mysqld/mysqld.sock)/php-crud-api"
	maxConnections   = 256
)

var (
	db *sql.DB
)

var (
	listenAddr = flag.String("listenAddr", ":8000", "Address to listen to")
	child      = flag.Bool("child", false, "is child proc")
)

func requestHandler(w http.ResponseWriter, req *http.Request) {
	msg := ""
	w.Header().Add("Content-Type", "application/json")

	method := req.Method
	u, _ := url.ParseRequestURI(req.RequestURI)
	request := strings.Split(strings.Trim(u.Path, "/"), "/")

	// load input from request body
	var input map[string]interface{}
	r := bufio.NewReader(req.Body)
	buf, _ := r.ReadBytes(0)
	json.Unmarshal(buf, &input)

	// retrieve the table and key from the path
	table := regexp.MustCompile("[^a-z0-9_]+").ReplaceAllString(request[0], "")
	key := 0
	if len(request) > 1 {
		key, _ = strconv.Atoi(request[1])
	}

	// escape the columns from the input object
	columns := make([]string, 0, len(input))
	var values []interface{}
	if key > 0 {
		values = make([]interface{}, 0, len(input)+1)
	} else {
		values = make([]interface{}, 0, len(input))
	}
	set := ""
	i := 0
	for column := range input {
		name := regexp.MustCompile("[^a-z0-9_]+").ReplaceAllString(column, "")
		columns[i] = name
		values[i] = input[column]
		if i > 0 {
			set += ", "
		}
		set += fmt.Sprintf("`%s`=@%d", name, i)
		i++
	}

	// create SQL based on HTTP method
	query := ""
	switch method {
	case "GET":
		if key > 0 {
			query = fmt.Sprintf("select * from `%s` where `id`=?", table)
		} else {
			query = fmt.Sprintf("select * from `%s`", table)
		}
		break
	case "PUT":
		query = fmt.Sprintf("update `%s` set %s where `id`=?", table, set)
		break
	case "POST":
		query = fmt.Sprintf("insert into `%s` set %s; select last_insert_id()", table, set)
		break
	case "DELETE":
		query = fmt.Sprintf("delete `%s` where `id`=?", table)
		break
	}

	if key > 0 {
		values = append(values, key)
	}

	if method == "GET" {
		rows, err := db.Query(query, values...)
		if err != nil {
			log.Fatal(err)
		}

		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		values := make([]interface{}, len(cols))
		for i, _ := range values {
			var value *string
			values[i] = &value
		}

		if key == 0 {
			msg += "["
		}
		first := true
		for rows.Next() {
			if first {
				first = false
			} else {
				msg += ","
			}
			err := rows.Scan(values...)
			if err != nil {
				log.Fatal(err)
			}
			b, err := json.Marshal(values)
			if err != nil {
				log.Fatal(err)
			}
			msg += string(b)
		}
		if key == 0 {
			msg += "]"
		}
	} else if method == "POST" {
	} else {
		result, err := db.Exec(query, values...)
		if err != nil {
			log.Fatal(err)
		}
		b, err := json.Marshal(result)
		if err != nil {
			log.Fatal(err)
		}
		msg += string(b)
	}

	fmt.Fprint(w, msg)
}

func initDB() {
	var err error
	db, err = sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	db.SetMaxIdleConns(maxConnections)
	db.SetMaxOpenConns(maxConnections)
}

func main() {
	var listener net.Listener
	flag.Parse()
	listener = doPrefork()

	initDB()

	http.HandleFunc("/", requestHandler)
	http.Serve(listener, nil)
}

func doPrefork() (listener net.Listener) {
	var err error
	var fl *os.File
	var tcplistener *net.TCPListener
	if !*child {
		var addr *net.TCPAddr
		addr, err = net.ResolveTCPAddr("tcp", *listenAddr)
		if err != nil {
			log.Fatal(err)
		}
		tcplistener, err = net.ListenTCP("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		fl, err = tcplistener.File()
		if err != nil {
			log.Fatal(err)
		}
		children := make([]*exec.Cmd, runtime.NumCPU()/2)
		for i := range children {
			children[i] = exec.Command(os.Args[0], "-child")
			children[i].Stdout = os.Stdout
			children[i].Stderr = os.Stderr
			children[i].ExtraFiles = []*os.File{fl}
			err = children[i].Start()
			if err != nil {
				log.Fatal(err)
			}
		}
		for _, ch := range children {
			var err error = ch.Wait()
			if err != nil {
				log.Print(err)
			}
		}
		os.Exit(0)
	} else {
		fl = os.NewFile(3, "")
		listener, err = net.FileListener(fl)
		if err != nil {
			log.Fatal(err)
		}
		runtime.GOMAXPROCS(2)
	}
	return listener
}


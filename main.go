package main

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dataSource = flag.String("datasource", "etl:etl@(localhost:3306)/kickstarter?parseTime=true", "database configuration")
		delete     = flag.Bool("delete", false, "delete all tables")
	)
	flag.Parse()

	db, err := sql.Open("mysql", *dataSource)
	if err != nil {
		return err
	}
	defer db.Close()

	if *delete {
		fmt.Print("Delete all data from kickstarter table? (y/n) ")
		r := bufio.NewReader(os.Stdin)
		answer, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		answer = strings.ToLower(string(answer))
		if !strings.Contains(answer, "y") {
			fmt.Println("Doing nothing")
			return nil
		}
		fmt.Println("Deleting all tables")
		if err := deleteTables(db); err != nil {
			return err
		}
		return nil
	}

	count, err := countDatabaseTables(db, "kickstarter")
	if err != nil {
		return fmt.Errorf("counting database tables: %v", err)
	}
	if count != 0 {
		fmt.Printf("Database is not empty (it has %d tables). Please delete all tables or run the program with --delete\n", count)
		return nil
	}

	file := "kickstarter-data/ks-projects-201801.csv.zip"
	zipr, err := zip.OpenReader(file)
	if err != nil {
		return fmt.Errorf("reading zip file %s: %v", file, err)
	}
	defer zipr.Close()

	for _, zf := range zipr.File {
		if zf.Name != "ks-projects-201801.csv" {
			break
		}
		f, err := zf.Open()
		if err != nil {
			return fmt.Errorf("reading data from %s: %v", file, err)
		}
		defer f.Close()

		fmt.Println("Extracting data from", zf.Name)
		data, err := extractData(f)
		if err != nil {
			return fmt.Errorf("extracting data: %v", err)
		}

		fmt.Println("Transforming data")
		kickstarts := transformData(data)

		fmt.Println("Creating tables")
		if err := createTables(db); err != nil {
			return err
		}

		fmt.Println("Loading data")
		if err := loadData(db, kickstarts); err != nil {
			return fmt.Errorf("loading data: %v", err)
		}
	}

	return nil
}

type Data struct {
	ID             int64
	Name           string
	Category       string
	MainCategory   string
	Currency       string
	Deadline       string
	Goal           string
	Launched       string
	Pledged        string
	State          string
	Backers        string
	Country        string
	PledgedUSD     string
	PledgedRealUSD string
	GoalRealUSD    string
}

func extractData(r io.Reader) ([]Data, error) {
	var dd []Data
	csvr := csv.NewReader(r)

	if _, err := csvr.Read(); err != nil { // Ignore CSV headers.
		return nil, err
	}
	for {
		row, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		d := Data{
			Name:           row[1],
			Category:       row[2],
			MainCategory:   row[3],
			Currency:       row[4],
			Deadline:       row[5],
			Goal:           row[6],
			Launched:       row[7],
			Pledged:        row[8],
			State:          row[9],
			Backers:        row[10],
			Country:        row[11],
			PledgedUSD:     row[12],
			PledgedRealUSD: row[13],
			GoalRealUSD:    row[14],
		}
		id, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %v", row[0], err)
		}
		d.ID = id
		dd = append(dd, d)
	}
	return dd, nil
}

func transformData(dd []Data) []Kickstart {
	var kk []Kickstart
	for i, d := range dd {
		id := int64(i + 1)

		product := Product{ID: id, KickstarterID: d.ID, Name: d.Name}
		mainCategory := MainCategory{ID: id, Name: d.MainCategory}
		category := Category{ID: id, Name: d.Category}
		currency := Currency{ID: id, Type: d.Currency}
		date := Date{ID: id, Launched: d.Launched, Deadline: d.Deadline}
		state := State{ID: id, State: d.State}
		area := Area{ID: id, Country: d.Country}

		k := Kickstart{
			Product:      product,
			MainCategory: mainCategory,
			Category:     category,
			Currency:     currency,
			Date:         date,
			State:        state,
			Area:         area,

			ProductID:      id,
			MainCategoryID: id,
			CategoryID:     id,
			CurrencyID:     id,
			DateID:         id,
			StateID:        id,
			AreaID:         id,

			Goal:       d.Goal,
			Backers:    d.Backers,
			Pledged:    d.Pledged,
			PledgedUSD: d.PledgedUSD,
		}
		kk = append(kk, k)
	}
	return kk
}

type Kickstart struct {
	Product      Product
	MainCategory MainCategory
	Category     Category
	Currency     Currency
	Date         Date
	State        State
	Area         Area

	ProductID      int64
	MainCategoryID int64
	CategoryID     int64
	CurrencyID     int64
	DateID         int64
	StateID        int64
	AreaID         int64

	Goal       string
	Pledged    string
	Backers    string
	PledgedUSD string
}

type Product struct {
	ID            int64
	KickstarterID int64
	Name          string
}

type MainCategory struct {
	ID   int64
	Name string
}

type Category struct {
	ID   int64
	Name string
}

type Currency struct {
	ID   int64
	Type string
}

type Date struct {
	ID       int64
	Launched string
	Deadline string
}

type State struct {
	ID    int64
	State string
}

type Area struct {
	ID      int64
	Country string
}

func createTables(db *sql.DB) error {
	const tableProducts = `
		CREATE TABLE IF NOT EXISTS products (
			id INT PRIMARY KEY AUTO_INCREMENT,
			kickstarter_id int unique,
			name varchar(255)
		)`
	if _, err := db.Exec(tableProducts); err != nil {
		return err
	}
	const tableKickstarts = `
		CREATE TABLE IF NOT EXISTS kickstarts (
			id INT PRIMARY KEY AUTO_INCREMENT,
			goal varchar(255),
			backers varchar(255),
			pledged varchar(255),
			pledged_usd varchar(255),
			product_id INT,
			FOREIGN KEY (product_id) REFERENCES products (id)
		)`
	if _, err := db.Exec(tableKickstarts); err != nil {
		return err
	}

	return nil
}

func deleteTables(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS kickstarts "); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS products "); err != nil {
		return err
	}
	return nil
}

func countDatabaseTables(db *sql.DB, database string) (int, error) {
	const query = `SELECT COUNT(DISTINCT table_name) FROM information_schema.columns WHERE table_schema = ?`
	var count int
	if err := db.QueryRow(query, database).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
func loadData(db *sql.DB, kk []Kickstart) error {
	for i, k := range kk {
		total := len(kk)
		percent := i * 100 / total
		fmt.Printf("\r%d/%d (%d%%)", i, total, percent)
		res, err := db.Exec("INSERT INTO products (kickstarter_id, name) values (?, ?)", k.Product.KickstarterID, k.Product.Name)
		if err != nil {
			return err
		}
		productID, err := res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = db.Exec("INSERT INTO kickstarts (product_id, goal, backers, pledged, pledged_usd) values (?, ?, ?, ?, ?)", productID, k.Goal, k.Backers, k.Pledged, k.PledgedUSD)
		if err != nil {
			return err
		}
	}
	return nil
}

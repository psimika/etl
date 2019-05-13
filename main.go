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
	"time"

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

	start := time.Now()
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
	elapsed := time.Since(start)
	fmt.Printf("Finished ETL in %v\n", elapsed)

	return nil
}

type Data struct {
	ID             int64
	Name           string
	Category       string
	MainCategory   string
	Currency       string
	Deadline       string
	Launched       string
	State          string
	Country        string
	Backers        int
	Pledged        float64
	PledgedUSD     float64
	PledgedUSDReal float64
	Goal           float64
	GoalUSDReal    float64
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
			Name:         row[1],
			Category:     row[2],
			MainCategory: row[3],
			Currency:     row[4],
			Deadline:     row[5],
			Launched:     row[7],
			State:        row[9],
			Country:      row[11],
		}

		id, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing id %s: %v", row[0], err)
		}
		d.ID = id

		goal, err := strconv.ParseFloat(row[6], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing goal %s: %v", row[6], err)
		}
		d.Goal = goal

		pledged, err := strconv.ParseFloat(row[8], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing pledged %s: %v", row[8], err)
		}
		d.Pledged = pledged

		backers, err := strconv.Atoi(row[10])
		if err != nil {
			return nil, fmt.Errorf("parsing backers %s: %v", row[10], err)
		}
		d.Backers = backers

		pledgedUSD, err := strconv.ParseFloat(row[12], 64)
		if err != nil {
			// Silently skip rows with empty pledgedUSD.
			continue
		}
		d.PledgedUSD = pledgedUSD

		pledgedUSDReal, err := strconv.ParseFloat(row[13], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing pledgedUSDReal %s: %v", row[13], err)
		}
		d.PledgedUSDReal = pledgedUSDReal

		goalUSDReal, err := strconv.ParseFloat(row[14], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing goalUSDReal %s: %v", row[14], err)
		}
		d.GoalUSDReal = goalUSDReal

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

			Backers:     d.Backers,
			Goal:        d.Goal,
			GoalUSDReal: d.GoalUSDReal,
			Pledged:     d.Pledged,
			PledgedUSD:  d.PledgedUSD,
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

	Backers        int
	Goal           float64
	GoalUSDReal    float64
	Pledged        float64
	PledgedUSD     float64
	PledgedUSDReal float64
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
	const tableMainCategories = `
		CREATE TABLE IF NOT EXISTS main_categories (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name varchar(255)
		)`
	if _, err := db.Exec(tableMainCategories); err != nil {
		return err
	}
	const tableCategories = `
		CREATE TABLE IF NOT EXISTS categories (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name varchar(255)
		)`
	if _, err := db.Exec(tableCategories); err != nil {
		return err
	}
	const tableStates = `
		CREATE TABLE IF NOT EXISTS states (
			id INT PRIMARY KEY AUTO_INCREMENT,
			state varchar(255)
		)`
	if _, err := db.Exec(tableStates); err != nil {
		return err
	}
	const tableAreas = `
		CREATE TABLE IF NOT EXISTS areas (
			id INT PRIMARY KEY AUTO_INCREMENT,
			country varchar(255)
		)`
	if _, err := db.Exec(tableAreas); err != nil {
		return err
	}
	const tableKickstarts = `
		CREATE TABLE IF NOT EXISTS kickstarts (
			id INT PRIMARY KEY AUTO_INCREMENT,
			backers INT,
			goal NUMERIC(12,2),
			pledged NUMERIC(12,2),
			pledged_usd NUMERIC(12,2),
			pledged_usd_real NUMERIC(12,2),
			product_id INT,
			main_category_id INT,
			category_id INT,
			state_id INT,
			area_id INT,
			FOREIGN KEY (product_id) REFERENCES products (id),
			FOREIGN KEY (main_category_id) REFERENCES main_categories (id),
			FOREIGN KEY (category_id) REFERENCES categories (id),
			FOREIGN KEY (state_id) REFERENCES states (id),
			FOREIGN KEY (area_id) REFERENCES areas (id)
		)`
	if _, err := db.Exec(tableKickstarts); err != nil {
		return fmt.Errorf("creating table kickstarts: %v", err)
	}

	return nil
}

func deleteTables(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS kickstarts"); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS products"); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS main_categories"); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS categories"); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS states"); err != nil {
		return err
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS areas"); err != nil {
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

		res, err = db.Exec("INSERT INTO main_categories (name) values (?)", k.MainCategory.Name)
		if err != nil {
			return err
		}
		mainCategoryID, err := res.LastInsertId()
		if err != nil {
			return err
		}

		res, err = db.Exec("INSERT INTO categories (name) values (?)", k.Category.Name)
		if err != nil {
			return err
		}
		categoryID, err := res.LastInsertId()
		if err != nil {
			return err
		}

		res, err = db.Exec("INSERT INTO states (state) values (?)", k.State.State)
		if err != nil {
			return err
		}
		stateID, err := res.LastInsertId()
		if err != nil {
			return err
		}

		res, err = db.Exec("INSERT INTO areas (country) values (?)", k.Area.Country)
		if err != nil {
			return err
		}
		areaID, err := res.LastInsertId()
		if err != nil {
			return err
		}

		const insertKickstarts = `INSERT INTO kickstarts (
			product_id,
			main_category_id,
			category_id,
			state_id,
			area_id,
			goal,
			backers,
			pledged,
			pledged_usd,
			pledged_usd_real
		) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		_, err = db.Exec(insertKickstarts, productID, mainCategoryID, categoryID, stateID, areaID, k.Goal, k.Backers, k.Pledged, k.PledgedUSD, k.PledgedUSDReal)
		if err != nil {
			return err
		}
	}
	fmt.Printf("\r%d/%d (100%%)\n", len(kk), len(kk))
	return nil
}

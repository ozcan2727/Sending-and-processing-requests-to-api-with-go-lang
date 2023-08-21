package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var db *sql.DB
var connStr = "postgresql://" + getEnv("DB_USER_NAME") + ":" + getEnv("DB_PASSWORD") + "@" + getEnv("DB_URL") + "/" + getEnv("DB_NAME") + "?sslmode=disable"

type Author struct {
	id        int    `json:"id,omitempty"`
	firstName string `json:"first_name,omitempty"`
	lastName  sql.NullString
	orcId     string `json:"orcid,omitempty"`
}

func getEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		fmt.Printf("%s not set\n", key)
	} else {
		return val
	}
	return ""
}

func init() {
	var err error

	db, err = sql.Open("postgres", connStr)

	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	// this will be printed in the terminal, confirming the connection to the database
	fmt.Println("The database is connected")
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

func getAuthor() {

}
func OrcIdQuering(_author Author) {
	client := &http.Client{}
	queryURLFormat := "https://pub.orcid.org/v3.0/expanded-search/?q=orcid%3A" + _author.orcId + "&start=0&rows=1"
	req, err := http.NewRequest("GET", queryURLFormat, nil)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
	}

	req.Header.Add("Accept", "application/vnd.orcid+json")
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
	}
	if err != nil {
		fmt.Println("No response from request")
	}

	body, err := ioutil.ReadAll(res.Body) // response body is []byte
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		fmt.Println("JSON parse hatası:", err)
		return
	}

	expandedResult, ok := data["expanded-result"].([]interface{})
	if !ok || len(expandedResult) == 0 {
		fmt.Println("expanded-result bulunamadı veya boş")
		return
	}
	firstItem, ok := expandedResult[0].(map[string]interface{})
	if !ok {
		fmt.Println("İlk öğe çözülemiyor")
		return
	}
	var givenNames, familyNames string

	if firstItem["given-names"] != nil {
		givenNames = firstItem["given-names"].(string)
	} else {
		if _author.lastName.String != "" {
			givenNames = _author.firstName
		} else {
			givenNames = ""
		}

	}

	if firstItem["family-names"] != nil {
		familyNames = firstItem["family-names"].(string)
	} else {
		if _author.lastName.String != "" {
			familyNames = _author.lastName.String
		} else {
			familyNames = ""
		}

	}
	if strings.ToLower(_author.firstName) != strings.ToLower(givenNames) || strings.ToLower(_author.lastName.String) != strings.ToLower(familyNames) {
		fmt.Println("Id:", _author.id, " Eski İsmi=", _author.firstName, " ", _author.lastName.String, " Yeni İsmi=", givenNames, " ", familyNames)
	}

	_author.firstName = givenNames
	_author.lastName.String = familyNames
	updateAuthor(_author)
}

func updateAuthor(author Author) {
	var err error
	if err = db.Ping(); err != nil {
		panic(err)
	}
	errorCount := 0
	for {
		// İlk güncelleme denemesi.
		_, updateErr := db.Exec(
			`UPDATE tbl_author SET first_name = $1, last_name = $2 WHERE id = $3`,
			author.firstName,
			author.lastName,
			author.id,
		)
		if updateErr != nil {
			// Hata durumunda "X" ekle ve tekrar dene.
			author.orcId += "X"
			errorCount++
			fmt.Println("dublicate autor: ", author.firstName, " ", author.lastName, "orcid:", author.orcId)
			if _, updateErr := db.Exec(
				`UPDATE tbl_author SET first_name = $1, last_name = $2 ,orcid=$3 WHERE id = $4`,
				author.firstName,
				author.lastName,
				author.orcId,
				author.id,
			); updateErr != nil {
				panic(updateErr)
			}
		} else {

			break
		}
	}
}
func main() {
	var err error
	defer db.Close()
	if err = db.Ping(); err != nil {
		panic(err)
		panic(err)
	}
	rows, err := db.Query(`select id,first_name,last_name,orcid from tbl_author where orcid is not null  order by id asc `)
	CheckError(err)
	defer rows.Close()
	counter := 0
	for rows.Next() {
		var authorId int
		var firstName string
		var lastName sql.NullString
		var orcId string
		err = rows.Scan(&authorId, &firstName, &lastName, &orcId)
		CheckError(err)
		var _author Author
		_author.id = authorId
		_author.firstName = firstName
		_author.lastName = lastName
		_author.orcId = orcId
		OrcIdQuering(_author)
		counter += 1
		if counter%15 == 0 {
			time.Sleep(1500 * time.Millisecond)
		}
	}
}

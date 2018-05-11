package sqlite3

import (
	"fmt"
	"log"

	"database/sql"

	"net/url"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"strings"
)

// Storage implements a SQLite3 storage backend for Colly
type Storage struct {
	// Filename indicates the name of the sqlite file to use
	Filename string
	// handle to the db
	dbh *sql.DB
	mu  sync.RWMutex // Only used for cookie methods.
}

// Init initializes the sqlite3 storage
func (s *Storage) Init() error {

	if s.dbh == nil {
		db, err := sql.Open("sqlite3", s.Filename)
		if err != nil {
			return fmt.Errorf("unable to open db file: %s", err.Error())
		}

		err = db.Ping()
		if err != nil {
			return fmt.Errorf("db init failure: %s", err.Error())
		}
		s.dbh = db
	}
	// create the data structures if necessary
	statement, _ := s.dbh.Prepare("CREATE TABLE IF NOT EXISTS visited (id INTEGER PRIMARY KEY, requestID INTEGER, visited INT)")
	_, err := statement.Exec()
	if err != nil {
		return err
	}
	statement, _ = s.dbh.Prepare("CREATE INDEX IF NOT EXISTS idx_visited ON visited (requestID)")
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, _ = s.dbh.Prepare("CREATE TABLE IF NOT EXISTS cookies (id INTEGER PRIMARY KEY, host TEXT, cookies TEXT)")
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, err = s.dbh.Prepare("CREATE INDEX IF NOT EXISTS idx_cookies ON cookies (host)")
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, err = s.dbh.Prepare("CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, data BLOB)")
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	return nil
}

// Clear removes all entries from the storage
func (s *Storage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statement, err := s.dbh.Prepare("DROP TABLE visited")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, err = s.dbh.Prepare("DROP TABLE cookies")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}

	statement, err = s.dbh.Prepare("DROP TABLE queue")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	return nil
}

//Close the db
func (s *Storage) Close() error {
	err := s.dbh.Close()
	return err
}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID uint64) error {
	statement, err := s.dbh.Prepare("INSERT INTO visited (requestID, visited) VALUES (?, 1)")
	if err != nil {
		return err
	}
	_, err = statement.Exec(requestID)
	if err != nil {
		return err
	}
	return nil
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	var count int
	statement, err := s.dbh.Prepare("SELECT COUNT(*) FROM visited where requestId = ?")
	if err != nil {
		return false, err
	}
	row := statement.QueryRow(requestID)
	err = row.Scan(&count)
	if err != nil {
		return false, err
	}
	if count >= 1 {
		return true, nil
	}
	return false, nil
}

// SetCookies implements colly/storage..SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	// TODO Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	s.mu.Lock()
	defer s.mu.Unlock()

	statement, err := s.dbh.Prepare("INSERT INTO cookies (host, cookies) VALUES (?,?)")
	if err != nil {
		log.Printf("SetCookies() .Set error %s", err)
	}
	_, err = statement.Exec(u.Host, cookies)
	if err != nil {
		log.Printf("SetCookies() .Set error %s", err)
	}

}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	// TODO Cookie methods currently have no way to return an error.
	var cookies string
	s.mu.RLock()

	//cookiesStr, err := s.Client.Get(s.getCookieID(u.Host)).Result()
	statement, err := s.dbh.Prepare("SELECT cookies FROM cookies where host = ?")
	if err != nil {
		log.Printf("Cookies() .Get error %s", err)
		return ""
	}
	row := statement.QueryRow(u.Host)

	err = row.Scan(&cookies)

	s.mu.RUnlock()

	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return ""
		}

		log.Printf("Cookies() .Get error %s", err)
	}

	return cookies
}

// AddRequest implements queue.Storage.AddRequest() function
func (s *Storage) AddRequest(r []byte) error {
	//return s.Client.RPush(s.getQueueID(), r).Err()
	statement, err := s.dbh.Prepare("INSERT INTO queue (data) VALUES (?)")
	if err != nil {
		return err
	}
	_, err = statement.Exec(r)
	if err != nil {
		return err
	}
	return nil
}

// GetRequest implements queue.Storage.GetRequest() function
func (s *Storage) GetRequest() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var blob []byte
	var id int
	statement, err := s.dbh.Prepare("SELECT min(id), data FROM queue")
	if err != nil {
		return nil, err
	}
	row := statement.QueryRow()
	err = row.Scan(&id, &blob)
	if err != nil {
		return nil, err
	}

	statement, err = s.dbh.Prepare("DELETE FROM queue where id = ?")
	_, err = statement.Exec(id)
	if err != nil {
		return nil, err
	}

	return blob, nil
}

// QueueSize implements queue.Storage.QueueSize() function
func (s *Storage) QueueSize() (int, error) {
	var count int
	statement, err := s.dbh.Prepare("SELECT COUNT(*) FROM queue")
	if err != nil {
		return 0, err
	}
	row := statement.QueryRow()
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

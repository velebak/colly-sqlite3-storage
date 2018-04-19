# colly-sqlite3-storage
A SQLite3 storage back end  for the Colly web crawling/scraping framework https://go-colly.org

Example Usage:

```go
package main

import (
	"log"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/extensions"
	"github.com/gocolly/colly/queue"
	"github.com/velebak/colly-sqlite3-storage/colly/sqlite3"
)

func main() {
	// Instantiate collector
	c := colly.NewCollector(
		// Allow requests only to www.example.com
		colly.AllowedDomains("www.example.com"),
		//colly.Async(true),
		//colly.Debugger(&debug.LogDebugger{}),
	)

	storage := &sqlite3.Sqlite3Storage{
		Filename: "./results.db",
	}

	defer storage.Close()

	err := c.SetStorage(storage)
	if err != nil {
		panic(err)
	}

	extensions.RandomUserAgent(c)
	extensions.Referrer(c)


	q, _ := queue.New(8, storage)
	q.AddURL("http://www.example.com")

	//c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 4})

	// Find and visit all links
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		e.Request.Visit(e.Attr("href"))
	})

	c.OnResponse(func(r *colly.Response) {
		log.Println(r.Request.URL, "\t", r.StatusCode)
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Println(r.Request.URL, "\t", r.StatusCode, "\nError:", err)
	})


	q.Run(c)
	log.Println(c)
}


```

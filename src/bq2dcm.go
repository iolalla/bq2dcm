package main

import (
	"fmt"
	"log"
    "time"
	"net/http"
	"strconv"
	"encoding/json"
    "cloud.google.com/go/bigquery"
    "cloud.google.com/go/storage"
    "golang.org/x/net/context"
    "google.golang.org/api/iterator"
    "google.golang.org/appengine"
)

type Cookie struct {
    List string `json:"LIST"`
    Cookie int64 `json:"cookie"`
    Date int64 `json:"date"`
}

func (x *Cookie) CSV() string {
    var value string = x.List + ", "
    value += strconv.FormatUint(uint64(x.Cookie), 10) + ", "
    value += strconv.FormatUint(uint64(x.Date), 10)
    return value
}

var BUCKETNAME = "nbolsadcmtest"
//var QUERY = "SELECT 'List_ID' as ListID, User_ID as uid, NOW() as timestamp FROM (SELECT *,User_ID AS index, ROW_NUMBER() OVER (PARTITION BY index) AS pos, FROM [cloud-se-es:dcm.p_activity_411205] where User_ID != '0' ) WHERE pos = 1"
var QUERY = "Select Ticker as ListID, Volumen as uid, NOW() as timestamp FROM [nbolsa-calculus:ibex35.megabolsa] LIMIT 10"

func main() {
	http.HandleFunc("/", handle)
	http.HandleFunc("/cron", cron)
	http.HandleFunc("/_ah/health" , healthCheck)
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
	appengine.Main()
}

func cron(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    //projectID := "YOUR_PROJECT_ID"
    //projectID := "cloud-se-es"
    projectID := "nbolsa-calculus"

    client, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        log.Fatal(err)
    }

    q := client.Query(QUERY)
    it, err := q.Read(ctx)
    if err != nil {
        log.Fatal(err)
    }
	q.QueryConfig.UseStandardSQL = true

    var Cookies []Cookie
    for {
        var row []bigquery.Value
        err := it.Next(&row)
        if err == iterator.Done {
            break
        }
        if err != nil {
            log.Fatal(err)
        }

		var Cookie Cookie
		Cookie.List = row[0].(string)
		Cookie.Cookie = row[1].(int64)
		Cookie.Date = row[2].(int64)

		Cookies = append(Cookies, Cookie)
    }
    saveToFile(ctx, Cookies)

	res, err := json.Marshal(Cookies)
	if err != nil {
		log.Fatal(err)
		return
	}
	w.Header().Set("Content-Type","application/json; charset=UTF-8")
    w.WriteHeader(http.StatusOK)
    w.Write(res)
}

func handle(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path != "/" {
                http.NotFound(w, r)
                return
        }
        fmt.Fprint(w, "Methods are query and cron")
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
        fmt.Fprint(w, "ok")
}

func saveToFile(ctx context.Context, Cookies []Cookie) {
    client, err := storage.NewClient(ctx)
    if err != nil {
        log.Fatalf("Failed to create new client: %v", err)
    }

    bucketName := BUCKETNAME
    bucket := client.Bucket(bucketName)
    t := time.Now()
    name := "File-"+ t.Format("20060102_150405")+ ".csv"

    obj := bucket.Object(name)

    w := obj.NewWriter(ctx)
    var cookie2print string = ""
    for cookie := range Cookies {
        cookie2print += Cookies[cookie].CSV() + "\n"
    }

    if _, err := fmt.Fprintf(w, cookie2print); err != nil {
        log.Fatal(err)
    }

    if err := w.Close(); err != nil {
        log.Fatal(err)
    }

    log.Printf("Bucket %v created.\n", bucketName)
}

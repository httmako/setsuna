package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/httmako/jote"
	_ "github.com/lib/pq"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	// "time"
	"context"
	// "os"
)

type Log struct {
	ID   float64
	Ts   string `json:"ts"`
	Host string `json:"host"`
	Message string `json:"message"`
	PMessage sql.NullString
	Doc  string `json:"doc"`
}

type Config struct {
	Port                int    `json:"Port"`
	SQLConnectionString string `json:"sqlconnectionstring"`
	SQLMaxConnections   int    `json:"sqlmaxconnections"`
}

/* TODO:
 *  search bar bigger, more spaced
 *  implement time based filtering for specific dates, e.g. ?st=20020101195959&et=2025xxxxx (or 2025.01.01 19:59:59)
 */

var db *sql.DB
var logger *slog.Logger

func main() {
	jote.ProfilingUntilTimeIfSet(30)
	logger = jote.CreateLogger("stdout")

	config := Config{}

	jote.ReadConfigYAML("config.yaml", &config)

	var err error
	db, err = sql.Open("postgres", config.SQLConnectionString)
	jote.Must(err)
	jote.Must(db.Ping())
	db.SetMaxOpenConns(5)

	mux := http.NewServeMux()
	RequestCounter := atomic.Uint64{}
	jote.AddMetrics(mux, "kagero", &RequestCounter)

	tmpl := template.Must(template.New("list").Parse(htmlSearch))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		jote.ExecuteTemplate(tmpl, w, "list", jote.H{})
	})

	mux.HandleFunc("GET /search", func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("q")
		page := getNumFromRequest(w, r, "p")
		perpage := getNumFromRequest(w, r, "m")
		if page == -1 || perpage == -1 {
			return
		}
		timespan := r.FormValue("t")
		if !IsValidTimespan(timespan) {
			http.Error(w, "ERROR: invalid timespan", 400)
			return
		}
		timestamps, counts := getRowCountForGraphic(r.Context(), timespan)
		jote.ExecuteTemplate(tmpl, w, "list", jote.H{
			"list":   getRows(r.Context(), query, page, perpage),
			"bar_ts": timestamps,
			"bar_c":  counts,
		})
	})

	tmplView := template.Must(template.New("view").Parse(htmlView))
	mux.HandleFunc("GET /view", func(w http.ResponseWriter, r *http.Request) {
		id := getNumFromRequest(w, r, "id")
		if id == -1 || id == 0 {
			return
		}
		jote.ExecuteTemplate(tmplView, w, "view", jote.H{
			"doc": getDoc(r.Context(), id),
		})
	})

	jote.RunMux(":"+strconv.Itoa(config.Port), jote.AddLoggingToMuxWithCounter(mux, logger, &RequestCounter), logger)
}

func IsValidTimespan(timespan string) bool {
	arr := strings.Split(timespan, " ")
	if len(arr) != 2 {
		return false
	}
	if _, err := strconv.Atoi(arr[0]); err != nil {
		return false
	}
	t := arr[1]
	if t != "minutes" && t != "days" && t != "seconds" && t != "hours" {
		return false
	}
	return true
}

func getNumFromRequest(w http.ResponseWriter, r *http.Request, key string) int {
	in := r.FormValue(key)
	ret := 0
	var err error

	if in == "" {
		return 0
	}
	ret, err = strconv.Atoi(in)
	if err != nil {
		http.Error(w, "ERROR: "+key+" is not a number", 400)
		return -1
	}
	if ret < 0 {
		http.Error(w, "ERROR: "+key+" is < 0", 400)
		return -1
	}
	return ret
}

func getDoc(ctx context.Context, id int) Log {
	var log Log
	jote.Must(db.QueryRowContext(ctx, "SELECT id,ts,doc FROM docs WHERE id=$1", id).Scan(&log.ID, &log.Ts, &log.Doc))
	return log
}

func getRows(ctx context.Context, q string, p int, m int) []Log {
	var logs []Log
	rows, err := getSearchSql(ctx, q, p, m)
	jote.Must(err)
	defer rows.Close()
	for rows.Next() {
		log := Log{}
		jote.Must(rows.Scan(&log.ID, &log.Ts, &log.Host, &log.PMessage))
		if log.PMessage.Valid {
			log.Message = log.PMessage.String
		}
		logs = append(logs, log)
	}
	jote.Must(rows.Err())
	jote.Must(rows.Close())
	return logs
}

var timeList = []string{"seconds", "minutes", "hours", "days"}

func GetDateTrunc(timespan string) string {
	arr := strings.Split(timespan, " ")
	num, _ := strconv.Atoi(arr[0])
	takeSmaller := 0
	if num < 3 {
		takeSmaller = 1
	}
	for i, l := range timeList {
		if l == arr[1] {
			return timeList[max(0, i-takeSmaller)]
		}
	}
	return "hours"
}

func getRowCountForGraphic(ctx context.Context, timespan string) (string, string) {
	var timestamps []string
	var counts []int
	dateTrunc := GetDateTrunc(timespan)
	fmt.Println("SELECT date_trunc('" + dateTrunc + "', ts) AS time_bucket, COUNT(*) AS row_count FROM docs WHERE ts > CURRENT_TIMESTAMP - INTERVAL '" + timespan + "' GROUP BY time_bucket ORDER BY time_bucket")
	rows, err := db.QueryContext(ctx, "SELECT date_trunc('"+dateTrunc+"', ts) AS time_bucket, COUNT(*) AS row_count FROM docs WHERE ts > CURRENT_TIMESTAMP - INTERVAL '"+timespan+"' GROUP BY time_bucket ORDER BY time_bucket")
	jote.Must(err)
	defer rows.Close()
	var ts string
	var c int
	for rows.Next() {
		jote.Must(rows.Scan(&ts, &c))
		timestamps = append(timestamps, ts)
		counts = append(counts, c)
	}
	jote.Must(rows.Err())
	jote.Must(rows.Close())
	tss, err := json.Marshal(timestamps)
	jote.Must(err)
	cs, err := json.Marshal(counts)
	jote.Must(err)
	return string(tss), string(cs)
}

// SELECT * FROM docs WHERE ts > '2026-01-08T19:03:03'
// SELECT date_trunc('hour', ts) AS time_bucket, COUNT(*) AS row_count FROM docs GROUP BY time_bucket ORDER BY time_bucket
func getSearchSql(ctx context.Context, query string, page int, maxperpage int) (*sql.Rows, error) {
	maxperpage = max(min(maxperpage, 500), 10)
	page = max(min(page, 5), 0)
	if query == "" {
		return db.QueryContext(ctx, "SELECT id, ts, doc->'_meta'->'host', doc->'message' FROM docs ORDER BY id DESC LIMIT $1 OFFSET $2", maxperpage, page*maxperpage)
	} else if strings.Contains(query, ": ") {
		return buildQuerySql(ctx, query, page, maxperpage)
	} else {
		//LIKE in msg field because no field:value given
		query = "%" + query + "%"
		return db.QueryContext(ctx, "SELECT id, ts, doc->'_meta'->'host', doc->'message' FROM docs WHERE doc LIKE $1 ORDER BY id DESC LIMIT $2 OFFSET $3", query, maxperpage, page*maxperpage)
	}
}

func buildQuerySql(ctx context.Context, q string, p int, pd int) (*sql.Rows, error) {
	// (a: "b c" and b: c) or a)
	argC := 1
	bracketOpen := 0
	expectAndOr := false
	expectValue := false
	args := strings.Split(q, " ")
	sqlquery := []string{"SELECT id,ts,doc->'_meta'->'host', doc->'message' FROM docs WHERE"}
	sqlargs := []any{}
	for _, a := range args {
		la := strings.ToLower(a)
		if a == "(" {
			bracketOpen++
			sqlquery = append(sqlquery, a)
			continue
		}
		if a == ")" {
			bracketOpen--
			sqlquery = append(sqlquery, a)
			continue
		}
		if a[0] == '(' {
			bracketOpen++
			sqlquery = append(sqlquery, "(")
			a = strings.TrimPrefix(a, "(")
		}
		if strings.HasSuffix(a, ":") {
			if expectValue {
				panic("expected value, got key (string ends with :)")
			}
			a = strings.TrimSuffix(a, ":")
			a = strings.ReplaceAll(a, ".", ",")
			a = "{" + a + "}"
			sqlquery = append(sqlquery, "src#>>$"+strconv.Itoa(argC))
			argC++
			sqlargs = append(sqlargs, a)
			expectValue = true
			expectAndOr = false
			continue
		}
		if la == "and" || la == "or" {
			if expectValue || !expectAndOr {
				panic("expected value, got AND/OR")
			}
			sqlquery = append(sqlquery, a)
			expectAndOr = false
			continue
		}
		if expectValue {
			compare := "="
			if strings.Contains(a, "%") {
				compare = "LIKE"
			}
			sqlquery = append(sqlquery, compare+" $"+strconv.Itoa(argC))
			argC++
			sqlargs = append(sqlargs, a)
			expectValue = false
			expectAndOr = true
			continue
		}
		//simple word, LIKE search
		a = "%" + a + "%"
		sqlquery = append(sqlquery, "msg LIKE $"+strconv.Itoa(argC))
		argC++
		sqlargs = append(sqlargs, a)
		expectAndOr = true

	}
	if bracketOpen != 0 {
		panic("invalid bracket count")
	}
	if expectValue {
		panic("expect value true, missing value for key?")
	}
	sqlquery = append(sqlquery, fmt.Sprintf("ORDER BY id DESC LIMIT %d OFFSET %d", pd, p*pd))
	fmt.Println("Query:", strings.Join(sqlquery, " "), sqlargs)
	return db.QueryContext(ctx, strings.Join(sqlquery, " "), sqlargs...)
}

var htmlView = `
{{ define "view" }}
<!DOCTYPE html>
<html lang="en">
<head>
<title>setsuna doc</title>
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:consolas,arial;}
#mainsimple{
text-align:left;
width:100%;
max-width:900px;
}
table{border-collapse:collapse;}
tr{border-bottom: 1px solid #000;}
tr:last-child{border-bottom: none;}
td, th, table{border: 1px solid black;}
th{border-bottom: 2px solid black;}
</style>
</head>
<body>
<div id="main">
<h1>setsuna doc</h1>
<p>ID: {{.doc.ID | printf "%.f"}}</p>
<p>Time: {{.doc.Ts}}</p>
JSON:<pre id="content"></pre>
<br><br>
Raw:
<p id="src">{{.doc.Doc}}</p>
<br>
</div>
</body>
<script>
const clsMap = [
[/^".*:$/, "red"],
[/^"/, "green"],
[/true|false/, "blue"],
[/null/, "magenta"],
[/.*/, "darkorange"],
]

function prettifyJson(text){
console.log("Parsing:",text)
let obj = JSON.parse(text);
return JSON.stringify(obj, null, 4)
.replace(/&/g, '&amp;')
.replace(/</g, '&lt;')
.replace(/>/g, '&gt;')
.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, match => '<span style="color:'+clsMap.find(([regex]) => regex.test(match))[1]+'">'+match+'</span>');
}

async function createJSON(){
    let text = document.getElementById("src").innerText;
    try{
        document.getElementById("content").innerHTML = prettifyJson(text);
    }catch(e){
        document.getElementById("content").innerHTML = e;
    };
}
createJSON();
</script>
</html>
{{end}}
`

var htmlSearch = `
{{ define "list" }}
<!DOCTYPE html>
<html lang="en">
<head>
<title>setsuna logs</title>
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:consolas,arial;}
#tab{
text-align:left;
width:100%;
max-width:900px;
}
.fl{float:left;padding-right:10px;}
table{border-collapse:collapse;}
tr{border-bottom: 1px solid #000;}
tr:last-child{border-bottom: none;}
td, th, table{border: 1px solid black;}
th{border-bottom: 2px solid black;}

 /* Dropdown Button */
.dropbtn {
  cursor: pointer;
}

/* The container <div> - needed to position the dropdown content */
.dropdown {
  position: relative;
  display: inline-block;
}

/* Dropdown Content (Hidden by Default) */
.dropc {
  display: none;
  position: absolute;
  background-color: #f1f1f1;
  min-width: 160px;
  box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
  z-index: 1;
}

/* Links inside the dropdown */
.dropc a {
  color: black;
  padding: 12px 16px;
  text-decoration: none;
  display: block;
}

/* Change color of dropdown links on hover */
.dropc a:hover {background-color: #ddd;}
.show {display:block;} 
</style>
</head>
<body>
<div id="main">
<h1>setsuna logs</h1>
<form action="/search" method="GET" id="f">
<div class="fl">Search<br><input type="text" id="q" name="q" placeholder="g.h: i"></div>
<!--<div class="fl">From<br><input type="datetime-local" id="df" name="df" step="1"></div>-->
<!--<div class="fl">To<br><input type="datetime-local" id="dt" name="dt" step="1"></div>-->
<div class="fl">Past time period<br> 
    <div class="dropdown">
      <input type="text" class="dropbtn" id="t" name="t" value="30 minutes">
      <div id="myDropdown" class="dropc">
        <a onclick="ChangeTimeSpan('30 minutes')">Last 30min</a>
        <a onclick="ChangeTimeSpan('3 hours')">Last 3h</a>
        <a onclick="ChangeTimeSpan('1 day')">Last 24h</a>
        <a onclick="ChangeTimeSpan('7 days')">Last 7d</a>
        <a onclick="ChangeTimeSpan('30 days')">Last 30d</a>
      </div>
    </div>
</div>
<div class="fl">Max results<br> 
    <div class="dropdown">
      <input type="text" class="dropbtn" id="m" name="m" value="20">
      <div id="myDropdown" class="dropc">
        <a onclick="ChangeMaxResults('20')">20</a>
        <a onclick="ChangeMaxResults('100')">100</a>
        <a onclick="ChangeMaxResults('500')">500</a>
      </div>
    </div>
</div>

<div class="fl"><br><button type="submit">search</button></div>
</form>
<br>
<canvas id="myChart" style="width:90%;height:320px"></canvas> 
<br>
<table id="tab">
<tr><th>ID</th><th>Time</th><th>Host</th><th>Message</th></tr>
{{range $k,$v := .list}}
<tr><td><a href="view?id={{$v.ID | printf "%.f"}}">{{$v.ID | printf "%.f"}}</a></td><td>{{$v.Ts}}</td><td>{{$v.Host}}</td><td>{{$v.Message}}</td></tr>
{{end}}
</table>
</div>
</body>
<script>
let lastDate = null;
window.addEventListener("load2", function() {
    var datetimeField = document.getElementById("d");
    var now = new Date();
    var utcString = now.toISOString().substring(0,19);
    var year = now.getFullYear();
    var month = now.getMonth() + 1;
    var day = now.getDate();
    var hour = now.getHours();
    var minute = now.getMinutes();
    var second = now.getSeconds();
    var localDatetime = year + "-" +
                      (month < 10 ? "0" + month.toString() : month) + "-" +
                      (day < 10 ? "0" + day.toString() : day) + "T" +
                      (hour < 10 ? "0" + hour.toString() : hour) + ":" +
                      (minute < 10 ? "0" + minute.toString() : minute) +
                      utcString.substring(16,19);
    datetimeField.value = localDatetime;
    lastDate = localDatetime;
});

//If date of the datetime picker was the same as the one when rendered the page => dont send it as timerange
document.getElementById("f").addEventListener("submit", function(event) {
    if (document.getElementById("d").value == lastDate) {
        document.getElementById("d").disabled = true;
    }
});


const urlParams = new URLSearchParams(window.location.search);
window.addEventListener("load", function() {
    SetUrlParamToElement("q");
    SetUrlParamToElement("t");
    SetUrlParamToElement("m");
    SetUrlParamToElement("d");
});
function SetUrlParamToElement(id) {
    if (urlParams.get(id)) {
        document.getElementById(id).value = urlParams.get(id);
    }
}


let dropdowns = document.getElementsByClassName("dropbtn");
for (var i = 0; i < dropdowns.length; i++) {
    dropdowns[i].addEventListener("click", function(e) {
        e.target.parentElement.children[1].classList.toggle("show"); 
    });
}

function ChangeTimeSpan(value) { document.getElementById("t").value = value; }
function ChangeMaxResults(value) { document.getElementById("m").value = value; }

// Close the dropdown menu if the user clicks outside of it
window.onclick = function(event) {
    if (event.target.matches('.dropbtn')) { return }
    var dropdowns = document.getElementsByClassName("dropc");
    for (var i = 0; i < dropdowns.length; i++) {
        dropdowns[i].classList.remove('show');
    }
} 
</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0"></script>
<script>
const xValues = JSON.parse({{.bar_ts}});
const yValues = JSON.parse({{.bar_c}});

const ctx = document.getElementById('myChart');

const sum = yValues.reduce((a, b) => a + b, 0);
const avg = (sum / yValues.length) || 0;

new Chart(ctx, {
  type: "bar",
  data: {
    labels: xValues,
    datasets: [{
      data: yValues
    }]
  },
  options: {
    scales: {
      y: {
        max: avg*1.5,
      }
    },
    plugins: {
      legend: {display: false},
      title: {
        display: true,
        text: "documents",
        font: {size: 16}
      }
    }
  }
});
</script>
</html>
{{end}}
`

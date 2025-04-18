package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"regexp"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sjmudd/mysql_defaults_file"
)

type Config struct {
	dump     io.Writer
	topN     int
	last     int
	interval float64
	delay    int
}

type NormalizePattern struct {
	re   *regexp.Regexp
	subs string
}

func (p *NormalizePattern) Normalize(q string) string {
	return p.re.ReplaceAllString(q, p.subs)
}

var normalizePatterns = []NormalizePattern{
	{regexp.MustCompile(` +`), " "},
	{regexp.MustCompile(`[+\-]?\b\d+\b`), "N"},
	{regexp.MustCompile(`\b0x[0-9A-Fa-f]+\b`), "0xN"},
	{regexp.MustCompile(`(\\')`), ""},
	{regexp.MustCompile(`(\\")`), ""},
	{regexp.MustCompile(`'[^']+'`), "S"},
	{regexp.MustCompile(`"[^"]+"`), "S"},
	{regexp.MustCompile(`(([NS]\s*,\s*){4,})`), "..."},
}

func processList(db *sql.DB) []string {
	//goland:noinspection SqlNoDataSourceInspection
	procList := "SHOW FULL PROCESSLIST"
	rows, err := db.Query(procList)

	var queries []string

	if err != nil {
		log.Println(err)
		return queries
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println("Error closing rows:", err)
		}
	}()

	cs, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	lcs := len(cs)
	if lcs != 8 && lcs != 9 {
		log.Fatal("Unknown columns: ", cs)
	}
	for rows.Next() {
		var dbUser, host, db, command, state, info *string
		var id, timeSec int
		if lcs == 8 {
			err = rows.Scan(&id, &dbUser, &host, &db, &command, &timeSec, &state, &info)
		} else {
			var progress interface{}
			err = rows.Scan(&id, &dbUser, &host, &db, &command, &timeSec, &state, &info, &progress)
		}
		if err != nil {
			log.Print(err)
			continue
		}
		if info != nil && *info != "" && *info != procList {
			queries = append(queries, *info)
		}
	}
	return queries
}

func normalizeQuery(query string) string {
	for _, pat := range normalizePatterns {
		query = pat.Normalize(query)
	}
	return query
}

type queryCount struct {
	q string
	c int64
}
type pairList []queryCount

func (pl pairList) Len() int {
	return len(pl)
}

func (pl pairList) Less(i, j int) bool {
	return pl[i].c > pl[j].c
}

func (pl pairList) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

type Summarizer interface {
	Update(queries []string)
	Show(out io.Writer, num int)
}

func showSummary(w io.Writer, sum map[string]int64, n int) {
	counts := make([]queryCount, 0, len(sum))
	for q, c := range sum {
		counts = append(counts, queryCount{q, c})
	}
	sort.Sort(pairList(counts))

	for i, p := range counts {
		if i >= n {
			break
		}
		_, err := fmt.Fprintf(w, "%4d %s\n", p.c, p.q)
		if err != nil {
			return
		}
	}
}

type summarizer struct {
	counts map[string]int64
}

func (s *summarizer) Update(queries []string) {
	if s.counts == nil {
		s.counts = make(map[string]int64)
	}
	for _, q := range queries {
		s.counts[q]++
	}
}

func (s *summarizer) Show(out io.Writer, num int) {
	showSummary(out, s.counts, num)
}

type recentSummarizer struct {
	last   int
	counts [][]queryCount
}

func (s *recentSummarizer) Update(queries []string) {
	if len(s.counts) >= s.last {
		s.counts = s.counts[1:]
	}
	sort.Strings(queries)
	qc := make([]queryCount, 0, 16)
	for _, q := range queries {
		if len(qc) > 0 && qc[len(qc)-1].q == q {
			qc[len(qc)-1].c++
		} else {
			qc = append(qc, queryCount{q: q, c: 1})
		}
	}
	s.counts = append(s.counts, qc)
}

func (s *recentSummarizer) Show(out io.Writer, num int) {
	sum := make(map[string]int64)
	for _, qcs := range s.counts {
		for _, qc := range qcs {
			sum[qc.q] += qc.c
		}
	}
	showSummary(out, sum, num)
}

func NewSummarizer(last int) Summarizer {
	if last > 0 {
		return &recentSummarizer{last: last}
	}
	return &summarizer{make(map[string]int64)}
}

func profile(db *sql.DB, cfg *Config) {
	summ := NewSummarizer(cfg.last)
	cnt := 0
	for {
		queries := processList(db)
		if cfg.dump != nil {
			for _, q := range queries {
				_, err := cfg.dump.Write([]byte(q))
				if err != nil {
					return
				}
				_, err2 := cfg.dump.Write([]byte{'\n'})
				if err2 != nil {
					return
				}
			}
		}

		for i, q := range queries {
			queries[i] = normalizeQuery(q)
		}
		summ.Update(queries)

		cnt++
		if cnt >= cfg.delay {
			cnt = 0
			fmt.Println("## ", time.Now().Local().Format("2006-01-02 15:04:05.00 -0700"))
			summ.Show(os.Stdout, cfg.topN)
		}

		time.Sleep(time.Duration(float64(time.Second) * cfg.interval))
	}
}

func main() {
	var host, dbuser, password, dumpfile string
	var port int

	cfg := Config{}
	flag.StringVar(&host, "host", "", "Host of database")
	flag.StringVar(&dbuser, "user", "", "User")
	flag.StringVar(&password, "password", "", "Password")
	flag.IntVar(&port, "port", 0, "Port")

	flag.StringVar(&dumpfile, "dump", "", "Write raw queries to this file")

	flag.IntVar(&cfg.topN, "top", 10, "(int) Show N most common queries")
	flag.IntVar(&cfg.last, "last", 0, "(int) Last N samples are summarized. 0 means summarize all samples")
	flag.Float64Var(&cfg.interval, "interval", 1.0, "(float) Sampling interval")
	flag.IntVar(&cfg.delay, "delay", 1, "(int) Show summary for each `delay` samples. -interval=0.1 -delay=30 shows summary for every 3sec")

	flag.Parse()

	// Initialize MySQL connection configuration
	// First try to load from ~/.my.cnf if it exists
	var config mysql_defaults_file.Config
	if _, err := os.Stat(os.Getenv("HOME") + "/.my.cnf"); err == nil {
		config = mysql_defaults_file.NewConfig("")
	}

	// Override with command line parameters if provided
	if host != "" {
		config.Host = host
	}
	if dbuser != "" {
		config.User = dbuser
	}
	if password != "" {
		config.Password = password
	} else if os.Getenv("MYSQL_PWD") != "" {
		config.Password = os.Getenv("MYSQL_PWD")
	}
	if port != 0 {
		config.Port = uint16(port)
	}

	// Set defaults for required fields if not provided
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.User == "" {
		currentUser, err := user.Current()
		if err == nil {
			config.User = currentUser.Name
		}
	}

	// Build the DSN (Data Source Name) for MySQL connection
	dsn := mysql_defaults_file.BuildDSN(config, "")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println("dsn: ", dsn)
		log.Fatal(err)
	}

	if dumpfile != "" {
		file, err := os.Create(dumpfile)
		if err != nil {
			log.Fatal(err)
		}
		cfg.dump = file
	}
	profile(db, &cfg)
}

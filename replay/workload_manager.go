package replay

import (
	"cassette-tape/db"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/pingcap/log"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/zap"
)

type workloadManager struct {
	workload     map[string][]workload
	duckdb       *db.DuckDB
	mysql        *db.MySQL
	readonly     bool
	wg           sync.WaitGroup
	totalQueries atomic.Uint64
	totalErrors  atomic.Uint64
}

func newWorkloadManager(duckdb *db.DuckDB, mysql *db.MySQL, readonly bool) *workloadManager {
	return &workloadManager{
		workload: make(map[string][]workload),
		wg:       sync.WaitGroup{},
		duckdb:   duckdb,
		mysql:    mysql,
		readonly: readonly,
	}
}

func (wm *workloadManager) run() error {
	err := wm.addWorkload()
	if err != nil {
		return err
	}

	wm.printTargetInfo()

	bo, err := confirm("🙇‍♀️ replay this workload? (y/n)")
	if err != nil {
		return err
	}

	totalQueries := 0
	for _, workloads := range wm.workload {
		totalQueries += len(workloads)
	}

	if bo {
		startTime := time.Now()
		progressBar := newProgressBar(totalQueries)
		for _, workloads := range wm.workload {
			wm.wg.Go(
				func() {
					wm.runWorkload(workloads, progressBar)
				},
			)
		}
		wm.wg.Wait()
		duration := time.Since(startTime)
		log.Info("replay completed",
			zap.Int("totalQueries", totalQueries),
			zap.String("duration", fmt.Sprintf("%.2fs", math.Round(duration.Seconds()*100)/100)),
			zap.Float64("qps",
				math.Round(float64(totalQueries-int(wm.totalErrors.Load()))/duration.Seconds()*100)/100),
			zap.Uint64("errors", wm.totalErrors.Load()),
		)
	} else {
		fmt.Println("👋 bye")
	}
	return nil
}

func (wm *workloadManager) runWorkload(ws []workload, bar *progressbar.ProgressBar) error {
	c, err := wm.mysql.Connect()
	if err != nil {
		return fmt.Errorf("connect mysql failed: %w", err)
	}
	defer c.Close()
	for _, w := range ws {
		_, err := c.Exec(w.text)
		if err != nil {
			wm.totalErrors.Add(1)
			fmt.Print("\r\033[K")
			log.Warn("",
				zap.String("sql", w.text),
				zap.String("reason", err.Error()),
			)
			err := bar.RenderBlank()
			if err != nil {
				return err
			}
		}
		err = bar.Add(1)
		if err != nil {
			return err
		}
		wm.totalQueries.Add(1)
	}
	return nil
}

func (wm *workloadManager) addWorkload() error {
	rs, err := wm.duckdb.Conn.Query(`SELECT conn FROM queries GROUP BY conn ORDER BY conn`)
	if err != nil {
		return fmt.Errorf("scan conns failed: %w", err)
	}
	defer rs.Close()

	query := `SELECT * FROM queries WHERE conn = ? ORDER BY timestamp`

	for rs.Next() {
		var c string
		if err := rs.Scan(&c); err != nil {
			return fmt.Errorf("scan conn failed: %w", err)
		}

		if wm.readonly {
			query = `SELECT * FROM queries WHERE conn = ? AND type = 'select' ORDER BY timestamp`
		}
		rs, err := wm.duckdb.Conn.Query(query, c)
		if err != nil {
			return fmt.Errorf("scan workload from conn failed: %w", err)
		}
		for rs.Next() {
			var w workload
			if err := rs.Scan(&w.timestamp, &w.conn, &w.tp, &w.digest, &w.text); err != nil {
				return fmt.Errorf("scan workload failed: %w", err)
			}
			wm.workload[c] = append(wm.workload[c], w)
		}
		log.Info("load workload completed",
			zap.String("thread", c),
			zap.Int("queries", len(wm.workload[c])))
	}
	return nil
}

func (wm *workloadManager) printTargetInfo() {
	log.Info("db",
		zap.String("host", wm.mysql.Host),
		zap.Int("port", wm.mysql.Port),
		zap.String("user", wm.mysql.User),
		zap.String("database", wm.mysql.Database),
	)
}

type workload struct {
	timestamp string
	conn      string
	tp        string
	digest    string
	text      string
}

func confirm(label string) (bool, error) {
	prompt := promptui.Prompt{
		Label: label,
		Validate: func(input string) error {
			if input != "y" && input != "n" {
				return fmt.Errorf("invalid input")
			}
			return nil
		},
	}
	b, err := prompt.Run()
	if err != nil {
		return false, err
	}
	return b == "y", nil
}

func newProgressBar(totalQueries int) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(totalQueries,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetWidth(16),
		progressbar.OptionSetDescription("✈️ replaying..."),
		progressbar.OptionOnCompletion(func() {
			fmt.Println()
		}),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return bar
}

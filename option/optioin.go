package replay

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/manifoldco/promptui"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Option struct {
	Name string
	Size string
}

const (
	kilobyte   = 1024
	megabyte   = kilobyte * 1024
	sizeFormat = "%.3f MB"
	suffix     = ".json"
)

func new(name string, size int64) Option {
	sizeMB := float64(size) / megabyte
	return Option{
		Name: name,
		Size: fmt.Sprintf(sizeFormat, sizeMB),
	}
}

func GetOption() (string, error) {
	os, err := getAll()
	if err != nil {
		return "", fmt.Errorf("get workload failed: %w", err)
	}
	i, _, err := newOptionPrompt(os)
	if err != nil {
		return "nil", fmt.Errorf("render promptui failed: %w", err)
	}
	return os[i].Name, nil
}

func getAll() ([]Option, error) {
	entries, err := os.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var option []Option
	for _, entry := range entries {
		if isJSON(entry) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			log.Warn("failed to get workload info",
				zap.String("option", entry.Name()), zap.Error(err))
			continue
		}
		option = append(option,
			new(entry.Name(), info.Size()))
	}

	if len(option) == 0 {
		return nil, fmt.Errorf("no workload found")
	}
	return option, nil
}

func isJSON(entry os.DirEntry) bool {
	return entry.IsDir() || filepath.Ext(entry.Name()) != suffix
}

func newOptionPrompt(os []Option) (int, string, error) {

	options := make([]string, 0)

	for _, o := range os {
		option := fmt.Sprintf("%s (%s)", o.Name, o.Size)
		options = append(options, option)
	}
	prompt := promptui.Select{
		Label: "🔍 select workload to replay",
		Items: options,
		Size:  10,
	}
	return prompt.Run()
}

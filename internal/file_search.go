package internal

import "os"

func SearchFileName(folder string) ([]string, error) {
	var (
		files []string
	)
	entries, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

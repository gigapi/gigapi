//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

const (
	defaultUIReleaseURL = "https://github.com/gigapi/gigapi-ui/releases/latest/download/release.zip"
	uiZipFile           = "ui.zip"
	targetDir           = "ui"
)

func main() {
	uiReleaseURL := getenv("UI_RELEASE_URL", defaultUIReleaseURL)
	zipFilePath := filepath.Join(targetDir, uiZipFile)

	// Download the UI zip file
	fmt.Println("Downloading UI release from:", uiReleaseURL)
	if err := downloadFile(uiReleaseURL, zipFilePath); err != nil {
		exitf("Error downloading UI: %v", err)
	}
}
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func exitf(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
	os.Exit(1)
}

func downloadFile(url, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}

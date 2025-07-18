package ui

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/spf13/afero"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
)

var distFS afero.Fs
var fileServer http.Handler

//go:embed ui.zip
var uiZip []byte

func Init(api modules.Api) {
	if !config.Config.Gigapi.UI {
		return
	}

	UIFS := afero.NewMemMapFs()
	err := unzipToMemFS(UIFS, uiZip)
	if err != nil {
		panic(err)
	}

	distFS = afero.NewBasePathFs(UIFS, "/dist")
	httpFS := afero.NewHttpFs(distFS)
	fileServer = http.FileServer(httpFS)

	api.RegisterRoute(&modules.Route{
		Path:    "/",
		Methods: []string{"GET", "OPTIONS"},
		Handler: HandleUI,
	})
	afero.Walk(UIFS, "/", func(path string, d fs.FileInfo, err error) error {
		if d == nil || len(path) <= 5 {
			return nil
		}
		api.RegisterRoute(&modules.Route{
			Path:    path[len("/dist"):],
			Methods: []string{"GET"},
			Handler: HandleUI,
		})
		return nil
	})
	api.RegisterRoute(&modules.Route{
		PathPrefix: "/ui",
		Methods:    []string{"GET", "OPTIONS"},
		Handler:    HandleUI,
	})
}

func unzipFileToMemFS(memFS afero.Fs, zipFile *zip.File, absPath string) error {
	rc, err := zipFile.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	file, err := memFS.Create(absPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, rc)
	return err
}

func unzipToMemFS(memFS afero.Fs, zipData []byte) error {
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return err
	}

	for _, zipFile := range zipReader.File {
		fpath := filepath.Clean("/" + zipFile.Name)

		root := "/"
		absPath := filepath.Join(root, fpath)
		if !strings.HasPrefix(absPath, filepath.Clean(root)) {
			log.Printf("Skipping file with invalid path: %s", zipFile.Name)
			continue
		}

		if zipFile.FileInfo().IsDir() {
			memFS.MkdirAll(absPath, zipFile.Mode())
			continue
		}
		err = unzipFileToMemFS(memFS, zipFile, absPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func HandleUI(w http.ResponseWriter, r *http.Request) error {
	addCORSHeaders(w)
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return nil
	}
	// Try to serve the requested file
	requestedPath := r.URL.Path
	if requestedPath == "/" || requestedPath == "" {
		content, err := distFS.Open("index.html")
		if err != nil {
			log.Printf("Error reading index.html: %v", err)
			http.Error(w, "Internal server error", http.StatusNotFound)
			return nil
		}
		w.WriteHeader(200)
		io.Copy(w, content)
		return nil
	}
	// Check if file exists in embedded FS
	_, err := distFS.Stat(path.Clean(requestedPath))
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return nil
	}
	fileServer.ServeHTTP(w, r)
	return nil
}

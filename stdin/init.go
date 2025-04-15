package stdin

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/gigapi/gigapi/merge/utils"
	"io"
	"os"
)

// Module to get a request from stdin ane execute it.
// Currently used to initialize docker build

func Init() {
	var useStdin bool
	flag.BoolVar(&useStdin, "stdin", false, "Use stdin as input")
	flag.Parse()

	if !useStdin {
		return
	}

	processStdin()
	fmt.Println("Input read from stdin. Exiting.")
	os.Exit(0)
}

func processStdin() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Reading from stdin. Press Ctrl+D (Unix) or Ctrl+Z (Windows) followed by Enter to end input.")

	content, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Sprintf("Error reading from stdin: %v", err))
	}

	db, err := utils.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		panic(fmt.Sprintf("Error connecting to DuckDB: %v", err))
	}

	_, err = db.Exec(string(content))
	if err != nil {
		panic(fmt.Sprintf("Error executing SQL: %v", err))
	}
}

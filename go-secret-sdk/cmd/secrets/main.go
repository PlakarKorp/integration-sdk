package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	sdk "github.com/PlakarKorp/go-secret-sdk"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-no-ping] [-config file.json] [-o opt=val] name path [args...]\n",
		filepath.Base(os.Args[0]))
	os.Exit(1)
}

func main() {
	var (
		opt_conffile string
		opt_noping   bool
		config       = make(map[string]string)
	)

	log.SetPrefix(filepath.Base(os.Args[0]) + ": ")
	log.SetFlags(0)

	flag.StringVar(&opt_conffile, "config", "", "config file")
	flag.BoolVar(&opt_noping, "noping", false, "skip ping")
	flag.Func("o", "", func(o string) error {
		k, v, ok := strings.Cut(o, "=")
		if !ok {
			return fmt.Errorf("expected key=value, got %q", o)
		}
		config[k] = v
		return nil
	})

	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 2 {
		log.Println("missing executable")
		usage()
	}

	if opt_conffile != "" {
		file, err := os.Open(opt_conffile)
		if err != nil {
			log.Fatalf("cannot open %s: %v", opt_conffile, err)
		}
		defer file.Close()

		var conf map[string]string
		err = json.NewDecoder(file).Decode(&conf)
		if err != nil {
			log.Fatalln("failed to decode json:", err)
		}

		for key, value := range conf {
			if _, ok := config[key]; !ok {
				config[key] = value
			}
		}
	}

	for key, value := range config {
		switch {
		case strings.HasPrefix(value, "env:"):
			value = os.Getenv(value[4:])
		case strings.HasPrefix(value, "cmd:"):
			out, err := exec.Command("/bin/sh", "-c", value[4:]).CombinedOutput()
			if err != nil {
				log.Fatalf("failed to exec %q: %v", value[4:], err)
			}
			value = strings.TrimRight(string(out), "\r\n")
		case strings.HasPrefix(value, "val:"):
			value = value[4:]
		default:
		}
		config[key] = value
	}

	ctx := context.Background()
	bin := flag.Arg(0)
	path := flag.Arg(1)
	args := flag.Args()[2:]
	sp, err := sdk.ExecSecretProvider(ctx, config, bin, args)
	if err != nil {
		log.Fatalln("Failed to exec secret provider:", err)
	}

	defer sp.Close(ctx)

	if !opt_noping {
		if err := sp.Ping(ctx); err != nil {
			log.Fatalln("ping failed:", err)
		}
	}

	val, err := sp.Resolve(ctx, path)
	if err != nil {
		log.Fatalf("failed to resolve %s: %v", path, err)
	}
	fmt.Println(val)
}

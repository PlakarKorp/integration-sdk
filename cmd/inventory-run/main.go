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

	"github.com/PlakarKorp/go-inventory-sdk/inventory"
	sdk "github.com/PlakarKorp/go-inventory-sdk/sdk/client"
	"github.com/PlakarKorp/pkg"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-config file] [-o key=value] inventory-cmd\n",
		filepath.Base(os.Args[0]))
	os.Exit(1)
}

func main() {
	var (
		opt_conffile string
		config       = make(map[string]string)
	)

	log.SetPrefix(filepath.Base(os.Args[0]) + ": ")
	log.SetFlags(0)

	flag.StringVar(&opt_conffile, "config", "", "config file")
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
	if flag.NArg() == 0 {
		log.Println("missing executable")
		usage()
	}

	if opt_conffile != "" {
		file, err := os.Open(opt_conffile)
		if err != nil {
			log.Fatalf("cannot open %s: %v", opt_conffile, err)
		}

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
	args := flag.Args()[1:]
	inv, err := sdk.ExecInventory(ctx, config, bin, args)
	if err != nil {
		log.Fatalln("ConnectPlugin failed:", err)
	}

	run(ctx, inv)

	if err := inv.Close(ctx); err != nil {
		log.Fatalln("inventory close failed:", err)
	}
}

func show(v any) string {
	buf, _ := json.MarshalIndent(v, "", "   ")
	return string(buf)
}

func run(ctx context.Context, inv inventory.Inventory) {
	c := make(chan *inventory.InventoryEntry)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for r := range c {
			if r.Class != pkg.ResourceClassUndefined {
				fmt.Println(show(r))
			}
		}
	}()

	if err := inv.List(ctx, c); err != nil {
		log.Fatalln("inventory list failed:", err)
	}

	<-done
}

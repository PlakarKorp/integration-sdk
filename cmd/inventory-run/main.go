package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcinventory struct {
	integration.Inventory
	conn *grpc.ClientConn
}

func (g *grpcinventory) Close(ctx context.Context) error {
	ret := g.Inventory.Close(ctx)
	if err := g.conn.Close(); err != nil {
		if ret == nil {
			ret = err
		}
	}
	return ret
}

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
	inv, err := ExecInventory(ctx, config, bin, args)
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

func run(ctx context.Context, inv integration.Inventory) {
	c := make(chan *integration.InventoryEntry)
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

func spawn(ctx context.Context, exe string, args []string) (*grpc.ClientConn, error) {
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Stderr = os.Stderr // let child's stderr pass through for logging

	wr, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	rd, err := cmd.StdoutPipe()
	if err != nil {
		wr.Close()
		return nil, err
	}

	stdin, ok := rd.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdin is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	stdout, ok := wr.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdout is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	if err := cmd.Start(); err != nil {
		wr.Close()
		rd.Close()
		return nil, err
	}

	conn := NewStdioConn(stdin, stdout, cmd)

	client, err := grpc.NewClient("127.0.0.1:0",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return conn, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(0),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return client, nil
}

func ExecInventory(ctx context.Context, params map[string]string, exe string, args []string) (integration.Inventory, error) {
	conn, err := spawn(ctx, exe, args)
	if err != nil {
		return nil, err
	}

	inv, err := client.NewGrpcInventory(ctx, conn, params)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &grpcinventory{
		Inventory: inv,
		conn:      conn,
	}, nil
}

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/server"
)

type rot13 struct {
	rot int
}

func newrot13(ctx context.Context, opts map[string]string) (integration.Provider, error) {
	r := rot13{13}
	if s, ok := opts["n"]; ok {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		r.rot = n
	}

	return &r, nil
}

func (r *rot13) Ping(ctx context.Context) error { return nil }

func (r *rot13) Resolve(ctx context.Context, handle string) (string, error) {
	var (
		in  = []byte(strings.ToLower(handle))
		out = make([]byte, len(in))
	)

	for i := range in {
		if in[i] == ' ' {
			out[i] = ' '
		} else if in[i] < 'a' || in[i] > 'z' {
			return "", fmt.Errorf("bad input!")
		} else {
			out[i] = 'a' + byte((int(in[i])-'a'+r.rot)%26)
		}
	}

	return string(out), nil
}

func (r *rot13) Close(ctx context.Context) error {
	return nil
}

func main() {
	server.Entrypoint(os.Args, newrot13)
}

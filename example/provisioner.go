package example

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/PlakarKorp/integration-sdk"
)

// localdir creates a directory a snapshot is restored into with the fs
// exporter. It is the smallest provisioner there is: no provider, no
// credentials, so a restore into a provisioned target can be tried anywhere.
type localdir struct {
	baseDir string
	parent  string
	prefix  string
	flavor  string
}

func NewProvisioner(ctx context.Context, params map[string]string) (integration.Provisioner, error) {
	p := &localdir{
		baseDir: params["base_dir"],
		parent:  params["parent"],
		prefix:  params["name_prefix"],
		flavor:  params["flavor"],
	}
	if p.baseDir == "" || !filepath.IsAbs(p.baseDir) {
		return nil, errors.New("base_dir: an absolute path is required")
	}
	if p.prefix == "" {
		p.prefix = "restore"
	}
	if strings.ContainsAny(p.prefix+p.parent, `/\`) {
		return nil, errors.New("name_prefix and parent must be single path components")
	}
	return p, nil
}

func (p *localdir) Ping(ctx context.Context) error {
	info, err := os.Stat(p.baseDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", p.baseDir)
	}
	return nil
}

func (p *localdir) Provision(ctx context.Context, req integration.ProvisionRequest) (*integration.ProvisionResponse, error) {
	name := p.prefix + "-" + time.Now().UTC().Format("20060102-150405")
	if job := req.Labels["plakman.job"]; job != "" {
		name += "-" + job
	}
	dir := filepath.Join(p.baseDir, p.parent, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// The flavor is a stand-in for what a cloud provisioner would size the
	// resource with; recording it makes the chosen value visible on disk.
	if p.flavor != "" {
		if err := os.WriteFile(filepath.Join(dir, ".flavor"), []byte(p.flavor+"\n"), 0o644); err != nil {
			return nil, err
		}
	}
	return &integration.ProvisionResponse{
		Handle:   dir,
		Protocol: "fs",
		Target:   map[string]string{"location": "fs://" + dir},
	}, nil
}

func (p *localdir) Deprovision(ctx context.Context, handle string) error {
	rel, err := filepath.Rel(p.baseDir, handle)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("%s is not under %s", handle, p.baseDir)
	}
	return os.RemoveAll(handle)
}

func (p *localdir) Close(ctx context.Context) error { return nil }

// flavors are the sizes the form offers, with the attributes a table shows.
var flavors = []integration.Option{
	{Value: "small", Label: "Small", State: integration.OptionAvailable,
		Attributes: map[string]string{"cpu": "1", "ram": "1073741824", "price": "0.01", "stock": "available"}},
	{Value: "medium", Label: "Medium", State: integration.OptionAvailable,
		Attributes: map[string]string{"cpu": "2", "ram": "4294967296", "price": "0.04", "stock": "available"}},
	{Value: "large", Label: "Large", State: integration.OptionWarning, Reason: "Low stock.",
		Attributes: map[string]string{"cpu": "8", "ram": "34359738368", "price": "0.32", "stock": "low"}},
	{Value: "huge", Label: "Huge", State: integration.OptionUnavailable, Reason: "Out of stock.",
		Attributes: map[string]string{"cpu": "32", "ram": "137438953472", "price": "1.28", "stock": "out"}},
}

// Resolver answers the two dynamic fields of the form: the flavors, a fixed
// list, and the directories under base_dir, which depend on another field.
type Resolver struct{}

func (Resolver) Resolve(ctx context.Context, req integration.ResolveRequest) (*integration.ResolveResponse, error) {
	switch req.Resolver {
	case "flavors":
		return resolveFlavors(req), nil
	case "dirs":
		return resolveDirs(req)
	default:
		return nil, fmt.Errorf("unknown resolver %q", req.Resolver)
	}
}

func resolveFlavors(req integration.ResolveRequest) *integration.ResolveResponse {
	res := &integration.ResolveResponse{
		Search: integration.SearchLocal,
		Columns: []integration.Column{
			{Key: "cpu", Label: "vCPU", Type: integration.ColumnNumber},
			{Key: "ram", Label: "RAM", Type: integration.ColumnBytes},
			{Key: "price", Label: "Price", Type: integration.ColumnMoney, Currency: "EUR", Per: "hour"},
			{Key: "stock", Label: "Stock", Type: integration.ColumnBadge,
				Tones: map[string]string{"available": "success", "low": "warning", "out": "danger"}},
		},
	}
	for _, f := range flavors {
		if len(req.Values) > 0 && !contains(req.Values, f.Value) {
			continue
		}
		res.Options = append(res.Options, f)
	}
	return res
}

func resolveDirs(req integration.ResolveRequest) (*integration.ResolveResponse, error) {
	baseDir := req.Params["base_dir"]
	if baseDir == "" {
		return nil, errors.New("base_dir is required")
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	res := &integration.ResolveResponse{Search: integration.SearchLocal}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if len(req.Values) > 0 && !contains(req.Values, e.Name()) {
			continue
		}
		res.Options = append(res.Options, integration.Option{
			Value: e.Name(), Label: e.Name(), State: integration.OptionAvailable,
		})
	}
	sort.Slice(res.Options, func(i, j int) bool { return res.Options[i].Value < res.Options[j].Value })
	return res, nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

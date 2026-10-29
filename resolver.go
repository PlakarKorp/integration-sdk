package integration

import "context"

// Resolver lists the values a field of a connector's form may take, by asking
// the provider. A connector serves it next to its own service. It never
// changes anything at the provider.
type Resolver interface {
	Resolve(ctx context.Context, req ResolveRequest) (*ResolveResponse, error)
}

// SearchMode tells who filters on the text a user types.
type SearchMode string

const (
	// SearchNone offers no search box.
	SearchNone SearchMode = ""
	// SearchLocal lets the form filter the full list it loaded.
	SearchLocal SearchMode = "local"
	// SearchRemote sends the text to the resolver.
	SearchRemote SearchMode = "remote"
)

// OptionState tells whether an option can be picked.
type OptionState string

const (
	OptionAvailable   OptionState = "available"
	OptionWarning     OptionState = "warning"
	OptionUnavailable OptionState = "unavailable"
)

// ColumnType tells the form how to format an attribute.
type ColumnType string

const (
	ColumnText    ColumnType = "text"
	ColumnNumber  ColumnType = "number"
	ColumnBytes   ColumnType = "bytes"
	ColumnMoney   ColumnType = "money"
	ColumnBadge   ColumnType = "badge"
	ColumnBoolean ColumnType = "boolean"
)

// ResolveRequest is one resolution. Params are the ones the form's UI schema
// maps to the resolver, secrets already resolved.
type ResolveRequest struct {
	Resolver string
	Params   map[string]string
	Search   string
	Filters  map[string][]string
	// Values asks for exactly these options, to judge a saved selection.
	Values []string
	Cursor string
	Limit  int
}

// ResolveFilter is a filter a resolver declares. No values means free input.
type ResolveFilter struct {
	Key    string
	Values []string
}

// Column describes an attribute of the options. Every attribute has one.
type Column struct {
	Key   string
	Label string
	Type  ColumnType
	// Unit follows a number.
	Unit string
	// Currency (ISO 4217) and Per (hour or month) qualify money.
	Currency string
	Per      string
	// Secondary is shown under the cell: another attribute.
	Secondary string
	// Hidden keeps an attribute for labels and search without showing it.
	Hidden bool
	// Tones map a badge value to success, warning, danger or neutral.
	Tones map[string]string
}

// Option is a value a field may take.
type Option struct {
	Value       string
	Label       string
	Description string
	State       OptionState
	// Reason explains a warning or an unavailable option.
	Reason string
	// Attributes are typed by the Columns of the response.
	Attributes map[string]string
}

// ResolveResponse is the options of one resolution, a page at a time when
// the resolver paginates.
type ResolveResponse struct {
	Columns    []Column
	Options    []Option
	NextCursor string
	Search     SearchMode
	Filters    []ResolveFilter
}

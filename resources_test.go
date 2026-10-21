package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceClassIsValid(t *testing.T) {
	valid := []ResourceClass{
		ResourceClassUndefined,
		ResourceClassUnknown,
		ResourceClassAnalytics,
		ResourceClassBlockStorage,
		ResourceClassCompute,
		ResourceClassDatabase,
		ResourceClassFileStorage,
		ResourceClassHypervisor,
		ResourceClassIdentity,
		ResourceClassMessaging,
		ResourceClassNetwork,
		ResourceClassObjectStorage,
		ResourceClassObservability,
		ResourceClassRegistry,
		ResourceClassSecurity,
		ResourceClassService,
	}
	for _, c := range valid {
		require.True(t, c.IsValid(), "ResourceClass(%q).IsValid()", c)
	}
	require.False(t, ResourceClass("bogus").IsValid())
}

func TestResourceSubClassIsValid(t *testing.T) {
	valid := []ResourceSubClass{
		ResourceSubClassUndefined,
		ResourceSubClassUnknown,
		ResourceSubClassAzBlob,
		ResourceSubClassFTP,
		ResourceSubClassGCS,
		ResourceSubClassIMAP,
		ResourceSubClassMongoDB,
		ResourceSubClassMySQL,
		ResourceSubClassPVC,
		ResourceSubClassPostgreSQL,
		ResourceSubClassProxmox,
		ResourceSubClassRedis,
		ResourceSubClassS3,
		ResourceSubClassSFTP,
	}
	for _, c := range valid {
		require.True(t, c.IsValid(), "ResourceSubClass(%q).IsValid()", c)
	}
	require.False(t, ResourceSubClass("bogus").IsValid())
}

func TestConnectorTypeIsValid(t *testing.T) {
	valid := []ConnectorType{
		ConnectorTypeImporter,
		ConnectorTypeExporter,
		ConnectorTypeStorage,
		ConnectorTypeSecretProvider,
		ConnectorTypeInventory,
	}
	for _, c := range valid {
		require.True(t, c.IsValid(), "ConnectorType(%q).IsValid()", c)
	}
	require.False(t, ConnectorType("bogus").IsValid())
}

func TestIsSubClassOf(t *testing.T) {
	tests := []struct {
		sub    ResourceSubClass
		parent ResourceClass
		want   bool
	}{
		{ResourceSubClassS3, ResourceClassObjectStorage, true},
		{ResourceSubClassGCS, ResourceClassObjectStorage, true},
		{ResourceSubClassAzBlob, ResourceClassObjectStorage, true},
		{ResourceSubClassPostgreSQL, ResourceClassDatabase, true},
		{ResourceSubClassMySQL, ResourceClassDatabase, true},
		{ResourceSubClassProxmox, ResourceClassHypervisor, true},
		{ResourceSubClassPVC, ResourceClassBlockStorage, true},
		{ResourceSubClassSFTP, ResourceClassService, true},
		// negative cases
		{ResourceSubClassS3, ResourceClassDatabase, false},
		{ResourceSubClassProxmox, ResourceClassObjectStorage, false},
		{ResourceSubClassS3, ResourceClassAnalytics, false}, // parent with empty subclass list
		{ResourceSubClassS3, ResourceClass("bogus"), false}, // unknown parent
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.sub.IsSubClassOf(tt.parent), "%q.IsSubClassOf(%q)", tt.sub, tt.parent)
	}
}

func TestGetClassTree(t *testing.T) {
	tree := GetClassTree()
	require.NotNil(t, tree)
	require.Contains(t, tree, ResourceClassObjectStorage)
}

// Every class that appears as a key in the class tree must itself be a
// valid class, and every subclass listed under it must be a valid
// subclass. This guards against the kind of drift where a constant is
// added to the tree but not to the validity slice (and vice versa).
func TestClassTreeConsistency(t *testing.T) {
	for class, subs := range GetClassTree() {
		require.True(t, class.IsValid(), "class %q is a key in the tree", class)
		for _, sub := range subs {
			require.True(t, sub.IsValid(), "subclass %q under %q", sub, class)
			require.True(t, sub.IsSubClassOf(class), "%q listed under %q", sub, class)
		}
	}
}

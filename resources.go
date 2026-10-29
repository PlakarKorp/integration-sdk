package integration

import "slices"

type ResourceClass string

type ResourceSubClass string

type ConnectorType string

const (
	ResourceClassUndefined     ResourceClass = ""
	ResourceClassUnknown       ResourceClass = "unknown"
	ResourceClassAnalytics     ResourceClass = "analytics"
	ResourceClassBlockStorage  ResourceClass = "block-storage"
	ResourceClassCompute       ResourceClass = "compute"
	ResourceClassDatabase      ResourceClass = "database"
	ResourceClassFileStorage   ResourceClass = "file-storage"
	ResourceClassHypervisor    ResourceClass = "hypervisor"
	ResourceClassIdentity      ResourceClass = "identity"
	ResourceClassMessaging     ResourceClass = "messaging"
	ResourceClassNetwork       ResourceClass = "network"
	ResourceClassObjectStorage ResourceClass = "object-storage"
	ResourceClassObservability ResourceClass = "observability"
	ResourceClassRegistry      ResourceClass = "registry"
	ResourceClassSecurity      ResourceClass = "security"
	ResourceClassService       ResourceClass = "service"

	ResourceSubClassUndefined     ResourceSubClass = ""
	ResourceSubClassUnknown       ResourceSubClass = "unknown"
	ResourceSubClassAzBlob        ResourceSubClass = "azblob"
	ResourceSubClassFTP           ResourceSubClass = "ftp"
	ResourceSubClassGCS           ResourceSubClass = "gcs"
	ResourceSubClassIMAP          ResourceSubClass = "imap"
	ResourceSubClassMongoDB       ResourceSubClass = "mongodb"
	ResourceSubClassMySQL         ResourceSubClass = "mysql"
	ResourceSubClassPVC           ResourceSubClass = "pvc"
	ResourceSubClassPostgreSQL    ResourceSubClass = "postgresql"
	ResourceSubClassProxmox       ResourceSubClass = "proxmox"
	ResourceSubClassRedis         ResourceSubClass = "redis"
	ResourceSubClassS3            ResourceSubClass = "s3"
	ResourceSubClassSecretManager ResourceSubClass = "secret-manager"
	ResourceSubClassSFTP          ResourceSubClass = "sftp"
	ResourceSubClassVMware        ResourceSubClass = "vmware"

	ConnectorTypeImporter       ConnectorType = "importer"
	ConnectorTypeExporter       ConnectorType = "exporter"
	ConnectorTypeStorage        ConnectorType = "storage"
	ConnectorTypeSecretProvider ConnectorType = "secret_provider"
	ConnectorTypeInventory      ConnectorType = "inventory"
	ConnectorTypeProvisioner    ConnectorType = "provisioner"
)

var resourceClasses []ResourceClass = []ResourceClass{
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

var resourceSubClasses []ResourceSubClass = []ResourceSubClass{
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
	ResourceSubClassSecretManager,
	ResourceSubClassVMware,
}

var connectorTypes []ConnectorType = []ConnectorType{
	ConnectorTypeExporter,
	ConnectorTypeImporter,
	ConnectorTypeStorage,
	ConnectorTypeSecretProvider,
	ConnectorTypeInventory,
	ConnectorTypeProvisioner,
}

var resourceClassTree map[ResourceClass][]ResourceSubClass = map[ResourceClass][]ResourceSubClass{
	ResourceClassAnalytics:     {},
	ResourceClassBlockStorage:  {ResourceSubClassPVC},
	ResourceClassCompute:       {},
	ResourceClassDatabase:      {ResourceSubClassPostgreSQL, ResourceSubClassMySQL, ResourceSubClassMongoDB, ResourceSubClassRedis},
	ResourceClassFileStorage:   {},
	ResourceClassHypervisor:    {ResourceSubClassProxmox, ResourceSubClassVMware},
	ResourceClassIdentity:      {},
	ResourceClassMessaging:     {},
	ResourceClassNetwork:       {},
	ResourceClassObjectStorage: {ResourceSubClassGCS, ResourceSubClassS3, ResourceSubClassAzBlob},
	ResourceClassObservability: {},
	ResourceClassRegistry:      {},
	ResourceClassSecurity:      {ResourceSubClassSecretManager},
	ResourceClassService:       {ResourceSubClassFTP, ResourceSubClassIMAP, ResourceSubClassSFTP},
}

func GetClassTree() map[ResourceClass][]ResourceSubClass {
	return resourceClassTree
}

func (c ResourceClass) IsValid() bool {
	return slices.Contains(resourceClasses, c)
}

func (c ResourceSubClass) IsValid() bool {
	return slices.Contains(resourceSubClasses, c)
}

func (c ResourceSubClass) IsSubClassOf(parent ResourceClass) bool {
	subclasses, found := resourceClassTree[parent]
	if !found {
		return false
	}
	return slices.Contains(subclasses, c)
}

func (c ConnectorType) IsValid() bool {
	return slices.Contains(connectorTypes, c)
}

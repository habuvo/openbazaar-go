package schema

import (
	"reflect"
	"testing"

	"github.com/ipfs/go-ipfs/repo/fsrepo"
	"io/ioutil"
	"time"
)

func TestGetApiConfig(t *testing.T) {
	config, err := GetAPIConfig(configFixture())
	if config.Username != "TestUsername" {
		t.Error("Expected TestUsername, got ", config.Username)
	}
	if config.Password != "TestPassword" {
		t.Error("Expected TestPassword, got ", config.Password)
	}
	if !config.Authenticated {
		t.Error("Expected Authenticated = true")
	}
	if len(config.AllowedIPs) != 1 || config.AllowedIPs[0] != "127.0.0.1" {
		t.Error("Expected AllowedIPs = [127.0.0.1]")
	}
	if config.CORS == nil {
		t.Error("Cors is not set")
	}
	if reflect.ValueOf(config.HTTPHeaders).Kind() != reflect.Map {
		t.Error("Headers is not a map")
	}
	if config.Enabled != true {
		t.Error("Enabled is not true")
	}
	if !config.SSL {
		t.Error("Expected SSL = true")
	}
	if config.SSLCert == "" {
		t.Error("Expected test SSL cert, got ", config.SSLCert)
	}
	if config.SSLKey == "" {
		t.Error("Expected test SSL key, got ", config.SSLKey)
	}
	if err != nil {
		t.Error("GetAPIAuthentication threw an unexpected error")
	}

	_, err = GetAPIConfig([]byte{})
	if err == nil {
		t.Error("GetAPIAuthentication didn`t throw an error")
	}
}

func TestGetWalletConfig(t *testing.T) {
	config, err := GetWalletConfig(configFixture())
	if config.FeeAPI != "https://btc.fees.openbazaar.org" {
		t.Error("FeeApi does not equal expected value")
	}
	if config.TrustedPeer != "127.0.0.1:8333" {
		t.Error("TrustedPeer does not equal expected value")
	}
	if config.Type != "spvwallet" {
		t.Error("Type does not equal expected value")
	}
	if config.Binary != "/path/to/bitcoind" {
		t.Error("Binary does not equal expected value")
	}
	if config.LowFeeDefault != 20 {
		t.Error("Expected low to be 20, got ", config.LowFeeDefault)
	}
	if config.MediumFeeDefault != 40 {
		t.Error("Expected medium to be 40, got ", config.MediumFeeDefault)
	}
	if config.HighFeeDefault != 60 {
		t.Error("Expected high to be 60, got ", config.HighFeeDefault)
	}
	if config.MaxFee != 2000 {
		t.Error("Expected maxFee to be 2000, got ", config.MaxFee)
	}
	if err != nil {
		t.Error("GetFeeAPI threw an unexpected error")
	}

	_, err = GetWalletConfig([]byte{})
	if err == nil {
		t.Error("GetFeeAPI didn't throw an error")
	}
}

func TestGetDropboxApiToken(t *testing.T) {
	dropboxApiToken, err := GetDropboxApiToken(configFixture())
	if dropboxApiToken != "dropbox123" {
		t.Error("dropboxApiToken does not equal expected value")
	}
	if err != nil {
		t.Error("GetDropboxApiToken threw an unexpected error")
	}

	dropboxApiToken, err = GetDropboxApiToken([]byte{})
	if dropboxApiToken != "" {
		t.Error("Expected empty string, got ", dropboxApiToken)
	}
	if err == nil {
		t.Error("GetDropboxApiToken didn't throw an error")
	}
}

func TestRepublishInterval(t *testing.T) {
	interval, err := GetRepublishInterval(configFixture())
	if interval != time.Hour*24 {
		t.Error("RepublishInterval does not equal expected value")
	}
	if err != nil {
		t.Error("RepublishInterval threw an unexpected error")
	}

	interval, err = GetRepublishInterval([]byte{})
	if interval != time.Second*0 {
		t.Error("Expected zero duration, got ", interval)
	}
	if err == nil {
		t.Error("GetRepublishInterval didn't throw an error")
	}
}

func TestGetResolverConfig(t *testing.T) {
	resolvers, err := GetResolverConfig(configFixture())
	if err != nil {
		t.Error("GetResolverUrl threw an unexpected error")
	}
	if resolvers.Id != "https://resolver.onename.com/" {
		t.Error("resolverUrl does not equal expected value")
	}
}

func TestExtendConfigFile(t *testing.T) {
	appSchema := MustNewCustomSchemaManager(SchemaContext{
		DataPath:        GenerateTempPath(),
		TestModeEnabled: true,
	})
	if err := appSchema.BuildSchemaDirectories(); err != nil {
		t.Fatal(err)
	}
	defer appSchema.DestroySchemaDirectories()
	if err := appSchema.InitializeDatabase(); err != nil {
		t.Fatal(err)
	}
	if err := appSchema.InitializeIPFSRepo(); err != nil {
		t.Fatal(err)
	}
	// Overwrite config with fixture
	err := ioutil.WriteFile(appSchema.DataPathJoin("config"), configFixture(), 0666)
	if err != nil {
		t.Fatal("Unexpected error while building config fixture:", err.Error())
	}

	r, err := fsrepo.Open(appSchema.DataPath())
	if err != nil {
		t.Fatal("fsrepo.Open threw an unexpected error", err)
	}
	config, err := GetWalletConfig(configFixture())
	if err != nil {
		t.Fatal(err)
	}
	newMaxFee := config.MaxFee + 1
	if err := extendConfigFile(r, "Wallet.MaxFee", newMaxFee); err != nil {
		t.Fatal("extendConfigFile threw an unexpected error:", err)
	}

	configFile, err := ioutil.ReadFile(appSchema.DataPathJoin("config"))
	if err != nil {
		t.Error(err)
	}
	config, _ = GetWalletConfig(configFile)
	if config.MaxFee != newMaxFee {
		t.Fatalf("Expected maxFee to be %v, got %v", newMaxFee, config.MaxFee)
	}
}

func configFixture() []byte {
	return []byte(`{
  "API": {
    "HTTPHeaders": null
  },
  "Addresses": {
    "API": "",
    "Announce": null,
    "Gateway": "/ip4/127.0.0.1/tcp/4002",
    "NoAnnounce": null,
    "Swarm": [
      "/ip4/0.0.0.0/tcp/4001",
      "/ip4/0.0.0.0/udp/4001/utp",
      "/ip6/::/tcp/4001",
      "/ip6/::/udp/4001/utp"
    ]
  },
  "Bootstrap": [
    "/ip4/107.170.133.32/tcp/4001/ipfs/QmboEn7ycZqb8sXH6wJunWE6d3mdT9iVD7XWDmCcKE9jZ5",
    "/ip4/139.59.174.197/tcp/4001/ipfs/QmZbLxbrPfGKjhFPwv9g7PkT5jL5DzQ8mF3iioByWMAprj",
    "/ip4/139.59.6.222/tcp/4001/ipfs/QmPZkv392E7VxumGSugQDEpfk6bHxfv271HTdVvdUu5Sod"
  ],
  "DataSharing": {
    "AcceptStoreRequests": false,
    "PushTo": [
      "QmZbLxbrPfGKjhFPwv9g7PkT5jL5DzQ8mF3iioByWMAprj",
      "QmPZkv392E7VxumGSugQDEpfk6bHxfv271HTdVvdUu5Sod"
    ]
  },
  "Datastore": {
    "BloomFilterSize": 0,
    "GCPeriod": "1h",
    "HashOnRead": false,
    "Spec": {
      "mounts": [
        {
          "child": {
            "path": "blocks",
            "shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
            "sync": true,
            "type": "flatfs"
          },
          "mountpoint": "/blocks",
          "prefix": "flatfs.datastore",
          "type": "measure"
        },
        {
          "child": {
            "compression": "none",
            "path": "datastore",
            "type": "levelds"
          },
          "mountpoint": "/",
          "prefix": "leveldb.datastore",
          "type": "measure"
        }
      ],
      "type": "mount"
    },
    "StorageGCWatermark": 90,
    "StorageMax": "10GB"
  },
  "Discovery": {
    "MDNS": {
      "Enabled": false,
      "Interval": 10
    }
  },
  "Dropbox-api-token": "dropbox123",
  "Experimental": {
    "FilestoreEnabled": false,
    "Libp2pStreamMounting": false,
    "ShardingEnabled": false
  },
  "Gateway": {
    "HTTPHeaders": null,
    "PathPrefixes": [],
    "RootRedirect": "",
    "Writable": false
  },
  "Identity": {
    "PeerID": "testID",
    "PrivKey": "testKey"
  },
  "Ipns": {
    "BackUpAPI": "",
    "QuerySize": 0,
    "RecordLifetime": "7d",
    "RepublishPeriod": "24h",
    "ResolveCacheSize": 128,
    "UsePersistentCache": true
  },
  "JSON-API": {
    "AllowedIPs": [
      "127.0.0.1"
    ],
    "Authenticated": true,
    "CORS": "*",
    "Enabled": true,
    "HTTPHeaders": null,
    "Password": "TestPassword",
    "SSL": true,
    "SSLCert": "/path/to/ssl.cert",
    "SSLKey": "/path/to/ssl.key",
    "Username": "TestUsername"
  },
  "Mounts": {
    "FuseAllowOther": false,
    "IPFS": "/ipfs",
    "IPNS": "/ipns"
  },
  "Reprovider": {
    "Interval": "",
    "Strategy": ""
  },
  "RepublishInterval": "24h",
  "Resolvers": {
    ".id": "https://resolver.onename.com/"
  },
  "SupernodeRouting": {
    "Servers": null
  },
  "Swarm": {
    "AddrFilters": null,
    "DisableBandwidthMetrics": false,
    "DisableNatPortMap": false,
    "DisableRelay": false,
    "EnableRelayHop": false
  },
  "Tour": {
    "Last": ""
  },
  "Wallet": {
    "Binary": "/path/to/bitcoind",
    "FeeAPI": "https://btc.fees.openbazaar.org",
    "HighFeeDefault": 60,
    "LowFeeDefault": 20,
    "MaxFee": 2000,
    "MediumFeeDefault": 40,
    "RPCPassword": "password",
    "RPCUser": "username",
    "TrustedPeer": "127.0.0.1:8333",
    "Type": "spvwallet"
  }
}`)
}

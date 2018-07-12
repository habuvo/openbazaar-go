package migrations_test

import (
	"os"
	"testing"

	"github.com/OpenBazaar/jsonpb"
	"github.com/OpenBazaar/openbazaar-go/repo/migrations"
	"github.com/OpenBazaar/openbazaar-go/test/factory"
)

const testMigration009Password = "letmein"

var (
	testMigration009SchemaStmts = []string{
		"DROP TABLE IF EXISTS cases;",
		"DROP TABLE IF EXISTS sales;",
		"DROP TABLE IF EXISTS purchases;",
		migrations.Migration009CreateCasesTable,
		migrations.Migration009CreateSalesTable,
		migrations.Migration009CreatePurchasesTable,
	}

	testMigration009FixtureStmts = []string{
		"INSERT INTO cases(caseID, buyerContract) VALUES('1', ?);",
		"INSERT INTO sales(orderID, contract) VALUES('1', ?);",
		"INSERT INTO purchases(orderID, contract) VALUES('1', ?);",
	}
)

func TestMigration009(t *testing.T) {
	// Setup
	os.Mkdir("./datastore", os.ModePerm)
	defer os.RemoveAll("./datastore")
	db, err := migrations.NewDB(".", testMigration009Password, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range testMigration009SchemaStmts {
		_, err = db.Exec(stmt)
		if err != nil {
			t.Fatal(err)
		}
	}

	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "    ",
		OrigName:     false,
	}
	contract := factory.NewDisputedContract()
	contract.VendorListings[0] = factory.NewCryptoListing("TETH")
	marshaledContract, err := marshaler.MarshalToString(contract)
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range testMigration009FixtureStmts {
		_, err = db.Exec(stmt, marshaledContract)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test migration up
	var m migrations.Migration009
	err = m.Up(".", testMigration009Password, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./repover")
	assertCorrectRepoVer(t, "10")

	for _, table := range []string{"cases", "sales", "purchases"} {
		results := db.QueryRow("SELECT coinType, paymentCoin FROM " + table + " LIMIT 1;")
		if err != nil {
			t.Fatal(err)
		}
		var coinType, paymentCoin string
		err = results.Scan(&coinType, &paymentCoin)
		if err != nil {
			t.Fatal(err)
		}
		if coinType != "TETH" {
			t.Fatal("Incorrect coinType for table", table+":", coinType)
		}
		if paymentCoin != "TBTC" {
			t.Fatal("Incorrect paymentCoin for table", table+":", paymentCoin)
		}
	}

	// Test migration down
	err = m.Down(".", testMigration009Password, true)
	if err != nil {
		t.Fatal(err)
	}
	assertCorrectRepoVer(t, "9")

	for _, table := range []string{"cases", "sales", "purchases"} {
		for _, column := range []string{"coinType", "paymentCoin"} {
			errStr := db.
				QueryRow("SELECT " + column + " FROM " + table + ";").
				Scan().
				Error()
			expectedErr := "no such column: " + column
			if errStr != expectedErr {
				t.Fatal("expected '" + expectedErr + "'")
			}
		}
	}
}

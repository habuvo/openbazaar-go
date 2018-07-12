package migrations

import (
	"database/sql"

	"github.com/OpenBazaar/jsonpb"
	"github.com/OpenBazaar/openbazaar-go/pb"
	_ "github.com/mutecomm/go-sqlcipher"
)

type Migration009 struct{}

var (
	Migration009CreateCasesTable     = "create table cases (caseID text primary key not null, buyerContract blob, vendorContract blob, buyerValidationErrors blob, vendorValidationErrors blob, buyerPayoutAddress text, vendorPayoutAddress text, buyerOutpoints blob, vendorOutpoints blob, state integer, read integer, timestamp integer, buyerOpened integer, claim text, disputeResolution blob, lastDisputeExpiryNotifiedAt integer not null default 0);"
	Migration009CreateSalesTable     = "create table sales (orderID text primary key not null, contract blob, state integer, read integer, timestamp integer, total integer, thumbnail text, buyerID text, buyerHandle text, title text, shippingName text, shippingAddress text, paymentAddr text, funded integer, transactions blob, needsSync integer, lastDisputeTimeoutNotifiedAt integer not null default 0);"
	Migration009CreateSalesIndex     = "create index index_sales on sales (paymentAddr, timestamp);"
	Migration009CreatePurchasesTable = "create table purchases (orderID text primary key not null, contract blob, state integer, read integer, timestamp integer, total integer, thumbnail text, vendorID text, vendorHandle text, title text, shippingName text, shippingAddress text, paymentAddr text, funded integer, transactions blob, lastDisputeTimeoutNotifiedAt integer not null default 0, lastDisputeExpiryNotifiedAt integer not null default 0, disputedAt integer not null default 0);"
)

func (Migration009) Up(repoPath string, dbPassword string, testnet bool) (err error) {
	db, err := NewDB(repoPath, dbPassword, testnet)
	if err != nil {
		return err
	}

	// Update DB schema
	err = withTransaction(db, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			"ALTER TABLE cases ADD COLUMN coinType text DEFAULT '';",
			"ALTER TABLE sales ADD COLUMN coinType text DEFAULT '';",
			"ALTER TABLE purchases ADD COLUMN coinType text DEFAULT '';",
			"ALTER TABLE cases ADD COLUMN paymentCoin text DEFAULT '';",
			"ALTER TABLE sales ADD COLUMN paymentCoin text DEFAULT '';",
			"ALTER TABLE purchases ADD COLUMN paymentCoin text DEFAULT '';",
		} {
			_, err := tx.Exec(stmt)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Update repover now that the schema is changed
	err = writeRepoVer(repoPath, 10)
	if err != nil {
		return err
	}

	// Update DB data on a best effort basis
	err = migration009UpdateTablesCoins(db, "cases", "caseID", "COALESCE(buyerContract, vendorContract) AS contract")
	if err != nil {
		return err
	}

	err = migration009UpdateTablesCoins(db, "sales", "orderID", "contract")
	if err != nil {
		return err
	}

	err = migration009UpdateTablesCoins(db, "purchases", "orderID", "contract")
	if err != nil {
		return err
	}

	return nil
}

func (Migration009) Down(repoPath string, dbPassword string, testnet bool) error {
	db, err := NewDB(repoPath, dbPassword, testnet)
	if err != nil {
		return err
	}

	err = withTransaction(db, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			"ALTER TABLE cases RENAME TO temp_cases;",
			Migration009CreateCasesTable,
			"INSERT INTO cases SELECT caseID, buyerContract, vendorContract, buyerValidationErrors, vendorValidationErrors, buyerPayoutAddress, vendorPayoutAddress, buyerOutpoints, vendorOutpoints, state, read, timestamp, buyerOpened, claim, disputeResolution, lastDisputeExpiryNotifiedAt FROM temp_cases;",
			"DROP TABLE temp_cases;",

			"ALTER TABLE sales RENAME TO temp_sales;",
			Migration009CreateSalesTable,
			Migration009CreateSalesIndex,
			"INSERT INTO sales SELECT orderID, contract, state, read, timestamp, total, thumbnail, buyerID, buyerHandle, title, shippingName, shippingAddress, paymentAddr, funded, transactions, needsSync, lastDisputeTimeoutNotifiedAt FROM temp_sales;",
			"DROP TABLE temp_sales;",

			"ALTER TABLE purchases RENAME TO temp_purchases;",
			Migration009CreatePurchasesTable,
			"INSERT INTO purchases SELECT orderID, contract, state, read, timestamp, total, thumbnail, vendorID, vendorHandle, title, shippingName, shippingAddress, paymentAddr, funded, transactions, lastDisputeTimeoutNotifiedAt, lastDisputeExpiryNotifiedAt, disputedAt FROM temp_purchases;",
			"DROP TABLE temp_purchases;",
		} {
			_, err := tx.Exec(stmt)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = writeRepoVer(repoPath, 9)
	if err != nil {
		return err
	}
	return nil
}

func migration009UpdateTablesCoins(db *sql.DB, table string, idColumn string, contractColumn string) error {
	type coinset struct {
		paymentCoin string
		coinType    string
	}

	// Get all records for table and store the coinset for each entry
	rows, err := db.Query("SELECT " + idColumn + ", " + contractColumn + " FROM " + table + ";")
	if err != nil {
		return err
	}
	defer rows.Close()

	coinsToSet := map[string]coinset{}
	for rows.Next() {
		var orderID, marshaledContract string

		err = rows.Scan(&orderID, &marshaledContract)
		if err != nil {
			return err
		}

		if marshaledContract == "" {
			continue
		}

		contract := &pb.RicardianContract{}
		if err := jsonpb.UnmarshalString(marshaledContract, contract); err != nil {
			return err
		}

		coinsToSet[orderID] = coinset{
			coinType:    coinTypeForContract(contract),
			paymentCoin: paymentCoinForContract(contract),
		}
	}

	// Update each row with the coins
	err = withTransaction(db, func(tx *sql.Tx) error {
		for id, coins := range coinsToSet {
			_, err := tx.Exec(
				"UPDATE "+table+" SET coinType = ?, paymentCoin = ? WHERE "+idColumn+" = ?",
				coins.coinType,
				coins.paymentCoin,
				id)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func paymentCoinForContract(contract *pb.RicardianContract) string {
	paymentCoin := contract.BuyerOrder.Payment.Coin
	if paymentCoin != "" {
		return paymentCoin
	}

	if len(contract.VendorListings[0].Metadata.AcceptedCurrencies) > 0 {
		paymentCoin = contract.VendorListings[0].Metadata.AcceptedCurrencies[0]
	}

	return paymentCoin
}

func coinTypeForContract(contract *pb.RicardianContract) string {
	coinType := ""

	if len(contract.VendorListings) > 0 {
		coinType = contract.VendorListings[0].Metadata.CoinType
	}

	return coinType
}

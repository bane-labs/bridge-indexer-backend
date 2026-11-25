package indexer

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestBridgeEventFamiliesAreGatedByActivationHeight(t *testing.T) {
	bridge := common.HexToAddress(mainnetNeoXBridgeAddress)
	message := common.HexToAddress(mainnetNeoXMessageBridgeAddress)
	for _, tc := range []struct {
		addr   common.Address
		sig    common.Hash
		height uint64
		ok     bool
	}{
		{bridge, sigGasDeposit, neoxBridgeNativeNamesFrom - 1, true},
		{bridge, sigGasDeposit, neoxBridgeNativeNamesFrom, false},
		{bridge, sigNativeDepositRoot, neoxBridgeNativeNamesFrom - 1, false},
		{bridge, sigNativeDepositRoot, neoxBridgeNativeNamesFrom, true},
		{bridge, sigTokenDeposit, 5, true}, // token events never changed
		{message, sigMessageSend, neoxMessageBridgeFrom - 1, false},
		{message, sigMessageSend, neoxMessageBridgeFrom, true},
		{common.HexToAddress("0x1234"), sigGasDeposit, 1 << 40, true}, // unknown contract: no history
	} {
		if err := checkNeoXLogVersion(true, tc.addr, tc.sig, tc.height); (err == nil) != tc.ok {
			t.Errorf("%s %s at %d: %v", tc.addr, tc.sig, tc.height, err)
		}
		// Testnet shares the addresses but not the upgrade history.
		if err := checkNeoXLogVersion(false, tc.addr, tc.sig, tc.height); err != nil {
			t.Errorf("non-mainnet %s %s at %d: %v", tc.addr, tc.sig, tc.height, err)
		}
	}
	for _, tc := range []struct {
		contract, name string
		height         uint64
		ok             bool
	}{
		{mainnetN3BridgeHash, "GasDeposit", n3BridgeNativeNamesFrom - 1, true},
		{mainnetN3BridgeHash, "GasDeposit", n3BridgeNativeNamesFrom, false},
		{mainnetN3BridgeHash, "NativeWithdrawal", n3BridgeNativeNamesFrom - 1, false},
		{mainnetN3BridgeHash, "NativeWithdrawal", n3BridgeNativeNamesFrom, true},
		{mainnetN3BridgeHash, "TokenDeposit", n3BridgeDeployedAt - 1, false},
		{"0x" + mainnetN3MessageBridgeHash, "Store", n3MessageBridgeDeployedAt, true},
		{mainnetN3MessageBridgeHash, "Store", n3MessageBridgeDeployedAt - 1, false},
	} {
		if err := checkN3NotificationVersion(true, tc.contract, tc.name, tc.height); (err == nil) != tc.ok {
			t.Errorf("%s %s at %d: %v", tc.contract, tc.name, tc.height, err)
		}
		if err := checkN3NotificationVersion(false, tc.contract, tc.name, tc.height); err != nil {
			t.Errorf("non-mainnet %s %s at %d: %v", tc.contract, tc.name, tc.height, err)
		}
	}
}

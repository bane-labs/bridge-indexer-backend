package indexer

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Bridge implementations before 3.0.0 named the native (GAS) bridge events
// Gas*. 3.0.0 renamed them Native* with the same fields, units (8-decimal
// amounts) and nonce sequences. Both families decode to the native bridge
// type. The activation heights below are mainnet facts established from
// chain state (N3 ContractManagement update counter and NEF, Neo X EIP-1967
// slot and Upgraded logs), see docs/operation-lifecycle.md.
var (
	sigGasWithdrawal    = crypto.Keccak256Hash([]byte("GasWithdrawal(uint256,address,uint256,address,bytes32,bytes32)"))
	sigGasDeposit       = crypto.Keccak256Hash([]byte("GasDeposit(uint256,address,uint256)"))
	sigGasClaim         = crypto.Keccak256Hash([]byte("GasClaim(uint256,address,uint256)"))
	sigGasClaimable     = crypto.Keccak256Hash([]byte("GasClaimable(uint256,address,uint256)"))
	sigGasDepositRoot   = crypto.Keccak256Hash([]byte("GasDepositRootUpdate(uint256,bytes32)"))
	sigGasBridgePause   = crypto.Keccak256Hash([]byte("GasBridgePause()"))
	sigGasBridgeUnpause = crypto.Keccak256Hash([]byte("GasBridgeUnpause()"))
)

// Mainnet contracts whose versions are known. Neo X testnet uses the same
// native contract addresses with a different upgrade history, so the rules
// below apply only when the connected chain is mainnet.
const (
	mainnetNeoXChainID uint64 = 47763     // eth_chainId 0xba93
	mainnetN3Network   uint32 = 860833102 // getversion protocol.network

	mainnetNeoXBridgeAddress        = "0x1212000000000000000000000000000000000004"
	mainnetNeoXMessageBridgeAddress = "0x1212000000000000000000000000000000000009"
	mainnetN3BridgeHash             = "bb19cfc864b73159277e1fd39694b3fd5fc613d2"
	mainnetN3MessageBridgeHash      = "de9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11"

	// Neo X Bridge: 1.0.0 at genesis, 2.0.0 at 655,999, 3.0.0 at 1,772,382.
	neoxBridgeNativeNamesFrom uint64 = 1_772_382
	// Neo X MessageBridge: a placeholder without events until the
	// message-bridge/1.0.0 implementation at 4,721,793.
	neoxMessageBridgeFrom uint64 = 4_721_793
	// N3 Bridge: deployed at 5,751,638 (probably 1.0.0), 2.0.0 at 6,304,759,
	// 3.0.0 at 6,877,325.
	n3BridgeDeployedAt        uint64 = 5_751_638
	n3BridgeNativeNamesFrom   uint64 = 6_877_325
	n3MessageBridgeDeployedAt uint64 = 8_328_582
)

func isNeoXGasSig(sig common.Hash) bool {
	switch sig {
	case sigGasWithdrawal, sigGasDeposit, sigGasClaim, sigGasClaimable, sigGasDepositRoot, sigGasBridgePause, sigGasBridgeUnpause:
		return true
	}
	return false
}

func isNeoXNativeSig(sig common.Hash) bool {
	switch sig {
	case sigNativeWithdrawal, sigNativeDeposit, sigNativeClaim, sigNativeClaimable, sigNativeDepositRoot, sigNativeBridgePause, sigNativeBridgeUnpause:
		return true
	}
	return false
}

// checkNeoXLogVersion rejects a log that the implementation active at its
// height could not have emitted: a Gas* event from 3.0.0 on, a Native* event
// before it, or any MessageBridge event while the genesis placeholder was
// active. Contracts other than the mainnet ones have no known history and
// accept both families, as does any chain other than mainnet.
func checkNeoXLogVersion(mainnet bool, address common.Address, sig common.Hash, height uint64) error {
	if !mainnet {
		return nil
	}
	switch address {
	case common.HexToAddress(mainnetNeoXBridgeAddress):
		if isNeoXGasSig(sig) && height >= neoxBridgeNativeNamesFrom {
			return fmt.Errorf("Gas* event at %d, after the 3.0.0 activation at %d", height, neoxBridgeNativeNamesFrom)
		}
		if isNeoXNativeSig(sig) && height < neoxBridgeNativeNamesFrom {
			return fmt.Errorf("Native* event at %d, before the 3.0.0 activation at %d", height, neoxBridgeNativeNamesFrom)
		}
	case common.HexToAddress(mainnetNeoXMessageBridgeAddress):
		if height < neoxMessageBridgeFrom {
			return fmt.Errorf("MessageBridge event at %d while the genesis placeholder was active (no verified ABI before %d)", height, neoxMessageBridgeFrom)
		}
	}
	return nil
}

func n3EventFamily(name string) string {
	switch {
	case strings.HasPrefix(name, "Gas"):
		return "gas"
	case strings.HasPrefix(name, "Native"):
		return "native"
	}
	return ""
}

// checkN3NotificationVersion applies the same rule to N3 bridge
// notifications on N3 mainnet. contract is the LE hash string.
func checkN3NotificationVersion(mainnet bool, contract, name string, height uint64) error {
	if !mainnet {
		return nil
	}
	switch strings.TrimPrefix(strings.ToLower(contract), "0x") {
	case mainnetN3BridgeHash:
		if height < n3BridgeDeployedAt {
			return fmt.Errorf("N3 Bridge notification at %d, before its deployment at %d", height, n3BridgeDeployedAt)
		}
		switch n3EventFamily(name) {
		case "gas":
			if height >= n3BridgeNativeNamesFrom {
				return fmt.Errorf("%s at %d, after the 3.0.0 activation at %d", name, height, n3BridgeNativeNamesFrom)
			}
		case "native":
			if height < n3BridgeNativeNamesFrom {
				return fmt.Errorf("%s at %d, before the 3.0.0 activation at %d", name, height, n3BridgeNativeNamesFrom)
			}
		}
	case mainnetN3MessageBridgeHash:
		if height < n3MessageBridgeDeployedAt {
			return fmt.Errorf("N3 MessageBridge notification at %d, before its deployment at %d", height, n3MessageBridgeDeployedAt)
		}
	}
	return nil
}

// errNotMainnet is returned by historical recovery on another network: its
// version table, claimable storage slot and claim implementations are
// mainnet facts.
func errNotMainnet(chain string, got, want uint64) error {
	return fmt.Errorf("historical recovery supports %s mainnet only (network %d, mainnet %d): the bridge version history it relies on is mainnet-specific", chain, got, want)
}

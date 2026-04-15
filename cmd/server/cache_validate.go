package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/bane-labs/bridge-indexer-backend/internal/indexer"
	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

type nonceCollector struct {
	groups map[string]map[uint64]struct{}
}

func newNonceCollector() *nonceCollector {
	return &nonceCollector{groups: make(map[string]map[uint64]struct{})}
}

func (c *nonceCollector) add(group string, nonce uint64) {
	if nonce == 0 {
		// Nonce 0 is valid, keep it.
	}
	set := c.groups[group]
	if set == nil {
		set = make(map[uint64]struct{})
		c.groups[group] = set
	}
	set[nonce] = struct{}{}
}

func (c *nonceCollector) addSource(op *types.BridgeOperation) {
	if op == nil {
		return
	}
	group := fmt.Sprintf("%s|%s|source|%s", op.BridgeType, op.Direction, op.SourceChain)
	if op.BridgeType == types.BridgeTypeToken {
		group += tokenLabel(op.TokenContract, op.DestTokenContract)
	}
	c.add(group, op.Nonce)
}

func (c *nonceCollector) addCompletion(bt types.BridgeType, dir types.Direction, nonce uint64, destToken string) {
	destChain := "neo_x"
	if dir == types.DirectionWithdrawal {
		destChain = "neo_n3"
	}
	group := fmt.Sprintf("%s|%s|completion|%s", bt, dir, destChain)
	if bt == types.BridgeTypeToken {
		group += fmt.Sprintf("|dest_token=%s", normToken(destToken))
	}
	c.add(group, nonce)
}

type cacheValidationStorer struct {
	collector *nonceCollector
}

func (s *cacheValidationStorer) UpdateOperationCompletion(
	bt types.BridgeType,
	dir types.Direction,
	nonce uint64,
	_ uint64,
	_ string,
	_ time.Time,
) error {
	s.collector.addCompletion(bt, dir, nonce, "")
	return nil
}

func (s *cacheValidationStorer) UpdateTokenOperationCompletionByDestToken(
	dir types.Direction,
	nonce uint64,
	destToken string,
	_ uint64,
	_ string,
	_ time.Time,
) error {
	s.collector.addCompletion(types.BridgeTypeToken, dir, nonce, destToken)
	return nil
}

func (s *cacheValidationStorer) UpdateMessageDepositMirror(
	nonce uint64,
	_ string,
	_ uint64,
	_ string,
	_ time.Time,
) error {
	// Mirror source-side visibility for neo_n3->neo_x message direction from
	// NeoX alt execution/root updates.
	s.collector.add("message|deposit|source|neo_n3", nonce)
	return nil
}

func tokenLabel(srcToken, dstToken *string) string {
	return fmt.Sprintf("|src=%s|dst=%s", normToken(ptrVal(srcToken)), normToken(ptrVal(dstToken)))
}

func ptrVal(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func normToken(v string) string {
	s := strings.TrimSpace(strings.ToLower(v))
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return "unknown"
	}
	return s
}

func newCacheValidateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cache-validate",
		Short: "Validate nonce continuity in block cache",
		RunE:  runCacheValidate,
	}
}

func runCacheValidate(cmd *cobra.Command, args []string) error {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	v := viper.New()
	cfg, err := loadConfig(v, logger)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.BlockCacheDir) == "" {
		return fmt.Errorf("BLOCK_CACHE_DIR is not set")
	}

	opts := badger.DefaultOptions(cfg.BlockCacheDir)
	opts.Logger = nil
	opts.ReadOnly = true
	opts.BypassLockGuard = true
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("open cache db: %w", err)
	}
	defer func() { _ = db.Close() }()

	collector := newNonceCollector()
	storer := &cacheValidationStorer{collector: collector}
	evmMessageSendSig := crypto.Keccak256Hash([]byte("MessageSend(uint256,address,bytes,bytes,bytes32,bytes32)"))
	neoxMessageBridgeAddr := common.HexToAddress(cfg.NeoXMessageBridge)
	n3MessageBridgeHash := strings.TrimPrefix(strings.ToLower(cfg.N3MessageBridge), "0x")

	neoxProc := indexer.NewNeoXEventProcessor(storer, cfg.NeoXTokenBridge, cfg.NeoXMessageBridge, logger)
	n3Proc, err := indexer.NewN3EventProcessor(
		storer,
		strings.TrimPrefix(cfg.N3NeoXBridge, "0x"),
		strings.TrimPrefix(cfg.N3MessageBridge, "0x"),
		logger,
	)
	if err != nil {
		return fmt.Errorf("build n3 processor: %w", err)
	}

	ctx := context.Background()
	var neoxLogCount, n3LogCount int

	// NeoX cached logs.
	if err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte("neox:logs:")})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				var logs []ethtypes.Log
				if err := json.Unmarshal(val, &logs); err != nil {
					return nil // skip malformed entries
				}
				for _, lg := range logs {
					neoxLogCount++
					collectNeoXMessageSourceTypes(collector, neoxMessageBridgeAddr, evmMessageSendSig, lg)
					op, _, err := neoxProc.ProcessLog(lg, time.Time{})
					if err != nil {
						continue
					}
					collector.addSource(op)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan neox cache: %w", err)
	}

	// N3 cached app logs.
	if err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte("n3:applog:")})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			txHash := strings.TrimPrefix(key, "n3:applog:")
			if err := item.Value(func(val []byte) error {
				var appLog result.ApplicationLog
				if err := json.Unmarshal(val, &appLog); err != nil {
					return nil // skip malformed entries
				}
				for _, exec := range appLog.Executions {
					for _, ev := range exec.Events {
						n3LogCount++
						collectN3MessageExpectedCompletions(collector, n3MessageBridgeHash, ev)
						op, err := n3Proc.ProcessEvent(ev, 0, txHash, time.Time{})
						if err != nil {
							continue
						}
						collector.addSource(op)
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan n3 cache: %w", err)
	}

	deriveMessageWithdrawalCompletionMissing(collector)
	printCacheValidationReport(ctx, cfg.BlockCacheDir, neoxLogCount, n3LogCount, collector)
	return nil
}

func printCacheValidationReport(_ context.Context, cacheDir string, neoxLogs, n3Events int, c *nonceCollector) {
	fmt.Printf("Cache Validation\n")
	fmt.Printf("Cache DB: %s\n", cacheDir)
	fmt.Printf("NeoX logs processed: %d\n", neoxLogs)
	fmt.Printf("N3 events processed: %d\n\n", n3Events)

	keys := make([]string, 0, len(c.groups))
	for k := range c.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	w := tabwriter.NewWriter(io.Writer(cmdOut{}), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "GROUP\tMIN_NONCE\tMAX_NONCE\tCOUNT\tMISSING_COUNT\tMISSING_RANGES")
	for _, k := range keys {
		set := c.groups[k]
		if len(set) == 0 {
			fmt.Fprintf(w, "%s\t-\t-\t0\t0\t-\n", k)
			continue
		}
		nonces := make([]uint64, 0, len(set))
		for n := range set {
			nonces = append(nonces, n)
		}
		sort.Slice(nonces, func(i, j int) bool { return nonces[i] < nonces[j] })
		minN, maxN := nonces[0], nonces[len(nonces)-1]
		missingRanges, missingCount := computeMissingRanges(nonces)
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n", k, minN, maxN, len(nonces), missingCount, missingRanges)
	}
	_ = w.Flush()
}

type cmdOut struct{}

func (cmdOut) Write(p []byte) (int, error) { return fmt.Print(string(p)) }

func collectNeoXMessageSourceTypes(
	c *nonceCollector,
	messageBridge common.Address,
	messageSendSig common.Hash,
	log ethtypes.Log,
) {
	if log.Address != messageBridge || len(log.Topics) < 2 || log.Topics[0] != messageSendSig {
		return
	}
	nonce := new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	msgType, ok := extractEvmMessageType(log.Data)
	if !ok {
		return
	}
	switch msgType {
	case 0:
		c.add("message|withdrawal|source_executable|neo_x", nonce)
	case 1:
		c.add("message|withdrawal|source_store_only|neo_x", nonce)
	case 2:
		c.add("message|withdrawal|source_result|neo_x", nonce)
	default:
		c.add(fmt.Sprintf("message|withdrawal|source_unknown_type=%d|neo_x", msgType), nonce)
	}
}

func extractEvmMessageType(data []byte) (uint64, bool) {
	readWord := func(off int) []byte {
		if off < 0 || off+32 > len(data) {
			return nil
		}
		return data[off : off+32]
	}
	metadataOffsetWord := readWord(0)
	if metadataOffsetWord == nil {
		return 0, false
	}
	metadataOffset := int(new(big.Int).SetBytes(metadataOffsetWord).Uint64())
	metadataLenWord := readWord(metadataOffset)
	if metadataLenWord == nil {
		return 0, false
	}
	metadataLen := int(new(big.Int).SetBytes(metadataLenWord).Uint64())
	start := metadataOffset + 32
	if metadataLen <= 0 || start+32 > len(data) || start+metadataLen > len(data) {
		return 0, false
	}
	msgType := new(big.Int).SetBytes(data[start : start+32]).Uint64()
	return msgType, true
}

func collectN3MessageExpectedCompletions(c *nonceCollector, messageBridgeHash string, ev state.NotificationEvent) {
	if ev.Name != "Store" {
		return
	}
	if strings.TrimPrefix(strings.ToLower(ev.ScriptHash.StringLE()), "0x") != messageBridgeHash {
		return
	}
	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok || len(arr) < 2 {
		return
	}
	nonceInt, err := arr[0].TryInteger()
	if err != nil || !nonceInt.IsUint64() {
		return
	}
	metadataBytes, err := arr[1].TryBytes()
	if err != nil || len(metadataBytes) == 0 {
		return
	}
	metaItem, err := stackitem.Deserialize(metadataBytes)
	if err != nil {
		return
	}
	metaArr, ok := metaItem.Value().([]stackitem.Item)
	if !ok || len(metaArr) < 1 {
		return
	}
	msgTypeInt, err := metaArr[0].TryInteger()
	if err != nil || !msgTypeInt.IsInt64() {
		return
	}

	nonce := nonceInt.Uint64()
	switch msgTypeInt.Int64() {
	case 0:
		c.add("message|withdrawal|expected_completion|neo_n3", nonce)
	case 1:
		c.add("message|withdrawal|stored_store_only|neo_n3", nonce)
	case 2:
		c.add("message|withdrawal|stored_result|neo_n3", nonce)
	default:
		c.add(fmt.Sprintf("message|withdrawal|stored_unknown_type=%s|neo_n3", msgTypeInt.String()), nonce)
	}
}

func deriveMessageWithdrawalCompletionMissing(c *nonceCollector) {
	expected := c.groups["message|withdrawal|expected_completion|neo_n3"]
	if len(expected) == 0 {
		return
	}
	actual := c.groups["message|withdrawal|completion|neo_n3"]
	for nonce := range expected {
		if _, ok := actual[nonce]; !ok {
			c.add("message|withdrawal|completion_missing|neo_n3", nonce)
		}
	}
}

func computeMissingRanges(sortedNonces []uint64) (string, int) {
	if len(sortedNonces) < 2 {
		return "-", 0
	}
	var ranges []string
	missingCount := 0
	for i := 1; i < len(sortedNonces); i++ {
		prev := sortedNonces[i-1]
		cur := sortedNonces[i]
		if cur <= prev+1 {
			continue
		}
		start := prev + 1
		end := cur - 1
		missingCount += int(end - start + 1)
		if start == end {
			ranges = append(ranges, fmt.Sprintf("%d", start))
		} else {
			ranges = append(ranges, fmt.Sprintf("%d-%d", start, end))
		}
	}
	if len(ranges) == 0 {
		return "-", 0
	}
	if len(ranges) > 12 {
		return strings.Join(ranges[:12], ",") + ",...", missingCount
	}
	return strings.Join(ranges, ","), missingCount
}

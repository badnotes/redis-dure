package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisConfig configuration struct
type RedisConfig struct {
	Addrs    []string
	Password string
	DB       int
}

// ExportConfig export configuration
type ExportConfig struct {
	OutputFile string
	KeyPattern string
}

// ImportConfig import configuration
type ImportConfig struct {
	InputFile string
}

// RedisData represents data in Redis
type RedisData struct {
	Key        string      `json:"key"`
	Type       string      `json:"type"`
	Value      interface{} `json:"value"`
	Expiration int64       `json:"expiration,omitempty"` // Use seconds as unit, 0 means never expire
}

var (
	ctx = context.Background()
)

func main() {
	// Parse command line arguments
	redisAddrs := flag.String("addrs", "localhost:6379", "Redis addresses, multiple addresses separated by commas (cluster mode)")
	password := flag.String("password", "", "Redis password")
	db := flag.Int("db", 0, "Redis database (non-cluster mode)")
	export := flag.Bool("export", false, "Export mode")
	importFlag := flag.Bool("import", false, "Import mode")
	output := flag.String("output", "redis_data.json", "Export file path")
	input := flag.String("input", "redis_data.json", "Import file path")
	keyPattern := flag.String("pattern", "*", "Key matching pattern (supports regular expressions)")
	cluster := flag.Bool("cluster", false, "Force cluster mode")
	overwrite := flag.Bool("overwrite", true, "Overwrite existing keys")
	batchSize := flag.Int("batch", 100, "Import batch size (optimize speed, number of keys per batch)")
	skipExisting := flag.Bool("skip-existing", false, "Skip existing keys during import") // New parameter

	flag.Parse()

	if !*export && !*importFlag {
		log.Fatal("Please specify mode: -export or -import")
	}

	// Parse Redis addresses
	addrs := strings.Split(*redisAddrs, ",")
	fmt.Printf("Connecting to Redis servers: %v\n", addrs)

	// Create Redis client
	var rdb redis.UniversalClient

	// Determine whether to use cluster mode
	useCluster := *cluster || len(addrs) > 1
	if useCluster {
		fmt.Println("Using cluster mode")
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Password:     *password,
			DialTimeout:  30 * time.Second, // Increase from 10s
			ReadTimeout:  60 * time.Second, // Increase from 30s
			WriteTimeout: 60 * time.Second, // Increase from 30s
			PoolSize:     50,               // Add pool size (default is 10 per node)
			MinIdleConns: 10,               // Minimum idle connections
			MaxRetries:   5,                // Increase retries
			// Enable cluster discovery and redirection
			ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
				// Use default cluster slot discovery
				return nil, nil
			},
			// Routing policy - automatically route to the correct node based on the key
			RouteByLatency: false,
			RouteRandomly:  false,
		})
	} else {
		fmt.Println("Using standalone mode")
		rdb = redis.NewClient(&redis.Options{
			Addr:         addrs[0],
			Password:     *password,
			DB:           *db,
			DialTimeout:  30 * time.Second,
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
			PoolSize:     50, // Add pool size
			MinIdleConns: 10,
			MaxRetries:   5,
		})
	}

	// Test connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	fmt.Printf("Redis connection successful\n")
	// defer rdb.Close()

	if *export {
		fmt.Printf("Starting data export, pattern: %s, output file: %s\n", *keyPattern, *output)
		if *keyPattern != "*" {
			fmt.Printf("Filter pattern enabled, will only export keys matching '%s'\n", *keyPattern)
		} else {
			fmt.Println("No filter enabled, exporting all keys")
		}

		exportConfig := ExportConfig{
			OutputFile: *output,
			KeyPattern: *keyPattern,
		}
		err := exportData(rdb, exportConfig)
		if err != nil {
			log.Fatalf("Export failed: %v", err)
		}
		fmt.Printf("Data successfully exported to %s\n", exportConfig.OutputFile)
	}

	if *importFlag {
		fmt.Printf("Starting data import, input file: %s\n", *input)
		fmt.Printf("Overwrite mode: %v, batch size: %d, skip existing keys: %v\n", *overwrite, *batchSize, *skipExisting)
		importConfig := ImportConfig{
			InputFile: *input,
		}
		err := importData(rdb, importConfig, useCluster, *overwrite, *batchSize, *skipExisting)
		if err != nil {
			log.Fatalf("Import failed: %v", err)
		}
		fmt.Printf("Data successfully imported from %s\n", importConfig.InputFile)
	}
	// Close client explicitly at the end
	if err := rdb.Close(); err != nil {
		log.Printf("Error closing Redis client: %v", err)
	}
}

// exportData exports Redis data to JSON file
func exportData(rdb redis.UniversalClient, config ExportConfig) error {
	// Create output file
	file, err := os.Create(config.OutputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	// Compile regular expression
	var patternRegex *regexp.Regexp
	if config.KeyPattern != "*" {
		// If the pattern is not a simple wildcard, try to compile it as a regular expression
		patternRegex, err = regexp.Compile(config.KeyPattern)
		if err != nil {
			// If compilation fails, try to convert the wildcard pattern to a regular expression
			fmt.Printf("Warning: Pattern '%s' is not a valid regular expression, attempting to convert to regular expression\n", config.KeyPattern)

			// Convert Redis-style wildcards to regular expression
			// * matches any characters, ? matches a single character
			regexPattern := strings.ReplaceAll(config.KeyPattern, "*", ".*")
			regexPattern = strings.ReplaceAll(regexPattern, "?", ".")
			regexPattern = "^" + regexPattern + "$"

			patternRegex, err = regexp.Compile(regexPattern)
			if err != nil {
				return fmt.Errorf("Unable to convert pattern to valid regular expression: %v", err)
			}
			fmt.Printf("Converted pattern '%s' to regular expression: %s\n", config.KeyPattern, regexPattern)
		} else {
			fmt.Printf("Using regular expression filter: %s\n", config.KeyPattern)
		}
	} else {
		fmt.Println("No filter used, exporting all keys")
	}

	var cursor uint64
	processed := 0
	totalScanned := 0

	fmt.Printf("Starting key scan...\n")

	// Use SCAN to iterate over all keys and process directly
	for {
		var keys []string
		keys, cursor, err = rdb.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			return err
		}

		totalScanned += len(keys)
		if len(keys) > 0 && totalScanned%1000 == 0 {
			fmt.Printf("Scanned %d keys...\n", totalScanned)
		}

		for _, key := range keys {
			// Filter keys
			if patternRegex != nil {
				// Use regular expression matching
				if !patternRegex.MatchString(key) {
					// Skip non-matching keys
					continue
				}
			}

			data, err := getKeyData(rdb, key)
			if err != nil {
				log.Printf("Failed to get data for key %s: %v", key, err)
				continue
			}

			if data != nil {
				err = encoder.Encode(data)
				if err != nil {
					log.Printf("Failed to encode data for key %s: %v", key, err)
					continue
				}

				processed++
				if processed%100 == 0 {
					fmt.Printf("Processed %d keys...\n", processed)
				}
			}
		}

		if cursor == 0 {
			break
		}
	}

	fmt.Printf("Export completed, total scanned %d keys, successfully exported %d keys\n", totalScanned, processed)
	return nil
}

// getKeyData gets data for a single key
func getKeyData(rdb redis.UniversalClient, key string) (*RedisData, error) {
	// Check if the key exists
	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, fmt.Errorf("Key does not exist: %s", key)
	}

	keyType, err := rdb.Type(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var value interface{}
	var expiration int64

	// Get expiration time (in seconds)
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	// TTL > 0 means the key has an expiration time
	// TTL == -1 means the key never expires
	// TTL == -2 means the key does not exist (but we have already checked existence)
	if ttl > 0 {
		expiration = int64(ttl.Seconds())
	} else if ttl == -1 {
		expiration = 0 // 0 means never expire
	}

	// Get value based on type
	switch keyType {
	case "string":
		value, err = rdb.Get(ctx, key).Result()
	case "list":
		value, err = rdb.LRange(ctx, key, 0, -1).Result()
	case "set":
		value, err = rdb.SMembers(ctx, key).Result()
	case "zset":
		// For sorted sets, we need to get both members and scores
		zMembers, err := rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		// Convert to array of maps for JSON serialization
		zsetData := make([]map[string]interface{}, len(zMembers))
		for i, member := range zMembers {
			zsetData[i] = map[string]interface{}{
				"member": member.Member,
				"score":  member.Score,
			}
		}
		value = zsetData
	case "hash":
		value, err = rdb.HGetAll(ctx, key).Result()
	default:
		return nil, fmt.Errorf("Unsupported key type: %s", keyType)
	}

	if err != nil {
		return nil, err
	}

	return &RedisData{
		Key:        key,
		Type:       keyType,
		Value:      value,
		Expiration: expiration,
	}, nil
}

// importData imports data from JSON file to Redis (optimized: introduces pipelining for batch processing, and supports skipping existing keys)
func importData(rdb redis.UniversalClient, config ImportConfig, isCluster bool, overwrite bool, batchSize int, skipExisting bool) error {
	file, err := os.Open(config.InputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var processed int
	var failed int
	var skipped int        // New: skip count
	var batch []*RedisData // Accumulate batch

	fmt.Printf("Starting data import...\n")

	// Read JSON data line by line and process in batches
	for decoder.More() {
		var data RedisData
		err := decoder.Decode(&data)
		if err != nil {
			return err
		}

		batch = append(batch, &data)

		// When batch size is reached, execute pipeline import
		if len(batch) >= batchSize {
			p, f, s := importBatch(rdb, batch, isCluster, overwrite, skipExisting)
			processed += p
			failed += f
			skipped += s
			batch = nil // Clear batch
			if (processed+skipped)%1000 == 0 {
				fmt.Printf("Processed %d keys (imported %d, skipped %d)...\n", processed+skipped, processed, skipped)
			}
		}
	}

	// Process remaining batch
	if len(batch) > 0 {
		p, f, s := importBatch(rdb, batch, isCluster, overwrite, skipExisting)
		processed += p
		failed += f
		skipped += s
	}

	fmt.Printf("Import completed, successfully imported %d keys, failed %d keys, skipped %d keys\n", processed, failed, skipped)
	return nil
}

// importBatch imports a batch of keys using pipeline (added skip logic)
func importBatch(rdb redis.UniversalClient, batch []*RedisData, isCluster bool, overwrite bool, skipExisting bool) (int, int, int) { // Returns processed, failed, skipped
	var errs []error
	var processed int
	var skipped int

	// If skip-existing is enabled, first batch check existence and filter batch
	if skipExisting && !isCluster {
		existsMap := make(map[string]bool, len(batch))
		cmds, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, data := range batch {
				pipe.Exists(ctx, data.Key)
			}
			return nil
		})
		if err != nil {
			// If check fails, fallback to not skipping, but log error
			log.Printf("Batch existence check failed: %v, continuing without skipping", err)
		} else {
			newBatch := make([]*RedisData, 0, len(batch))
			for i, cmd := range cmds {
				exists := cmd.(*redis.IntCmd).Val()
				key := batch[i].Key
				if exists > 0 {
					existsMap[key] = true
					skipped++
					fmt.Printf("Skipping existing key: %s\n", key)
				} else {
					newBatch = append(newBatch, batch[i])
				}
			}
			batch = newBatch // Update batch to only non-existing keys
		}
	} else if skipExisting && isCluster {
		// 集群模式下逐个检查键是否存在
		newBatch := make([]*RedisData, 0, len(batch))
		for _, data := range batch {
			exists, err := rdb.Exists(ctx, data.Key).Result()
			if err != nil {
				log.Printf("检查键存在性失败 %s: %v", data.Key, err)
				// 出错时默认不跳过
				newBatch = append(newBatch, data)
				continue
			}
			if exists > 0 {
				skipped++
				fmt.Printf("跳过已存在的键: %s\n", data.Key)
			} else {
				newBatch = append(newBatch, data)
			}
		}
		batch = newBatch
	}

	// Now process the filtered batch
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, data := range batch {
			// Check and delete existing key (if overwrite)
			if overwrite {
				pipe.Del(ctx, data.Key)
			} else {
				exists, _ := pipe.Exists(ctx, data.Key).Result() // Ignore error, continue
				if exists > 0 {
					// Check type matching
					existingType, _ := pipe.Type(ctx, data.Key).Result()
					if existingType != data.Type {
						errs = append(errs, fmt.Errorf("Key %s already exists and type does not match: existing %s, import %s", data.Key, existingType, data.Type))
						continue
					}
				}
			}

			// Set value based on type (optimized for batch commands)
			switch data.Type {
			case "string":
				val, ok := data.Value.(string)
				if !ok {
					errs = append(errs, fmt.Errorf("Key %s: String value type error", data.Key))
					continue
				}
				pipe.Set(ctx, data.Key, val, time.Duration(data.Expiration)*time.Second) // Set expiration directly in Set

			case "list":
				vals, ok := data.Value.([]interface{})
				if !ok {
					errs = append(errs, fmt.Errorf("Key %s: List value type error", data.Key))
					continue
				}
				listVals := make([]interface{}, len(vals))
				for i, v := range vals {
					val, ok := v.(string)
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: List element type error", data.Key))
						continue
					}
					listVals[i] = val
				}
				pipe.RPush(ctx, data.Key, listVals...) // RPUSH all at once

			case "set":
				vals, ok := data.Value.([]interface{})
				if !ok {
					errs = append(errs, fmt.Errorf("Key %s: Set value type error", data.Key))
					continue
				}
				setMembers := make([]interface{}, len(vals))
				for i, v := range vals {
					val, ok := v.(string)
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: Set element type error", data.Key))
						continue
					}
					setMembers[i] = val
				}
				pipe.SAdd(ctx, data.Key, setMembers...) // SADD all at once

			case "zset":
				vals, ok := data.Value.([]interface{})
				if !ok {
					errs = append(errs, fmt.Errorf("Key %s: Sorted set value type error", data.Key))
					continue
				}
				zMembers := make([]*redis.Z, len(vals))
				for i, v := range vals {
					member, ok := v.(map[string]interface{})
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: Sorted set element type error", data.Key))
						continue
					}
					score, ok := member["score"].(float64)
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: Sorted set score type error", data.Key))
						continue
					}
					mem, ok := member["member"].(string)
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: Sorted set member type error", data.Key))
						continue
					}
					zMembers[i] = &redis.Z{Score: score, Member: mem}
				}
				pipe.ZAdd(ctx, data.Key, zMembers...) // ZADD all members at once

			case "hash":
				vals, ok := data.Value.(map[string]interface{})
				if !ok {
					errs = append(errs, fmt.Errorf("Key %s: Hash value type error", data.Key))
					continue
				}
				hashMap := make(map[string]interface{})
				for k, v := range vals {
					val, ok := v.(string)
					if !ok {
						errs = append(errs, fmt.Errorf("Key %s: Hash field value type error", data.Key))
						continue
					}
					hashMap[k] = val
				}
				pipe.HSet(ctx, data.Key, hashMap) // HSET all fields at once

			default:
				errs = append(errs, fmt.Errorf("Key %s: Unsupported key type: %s", data.Key, data.Type))
				continue
			}

			// For non-string types, set expiration separately (string is set in Set)
			if data.Type != "string" && data.Expiration > 0 {
				pipe.Expire(ctx, data.Key, time.Duration(data.Expiration)*time.Second)
			}
		}
		return nil
	})

	if err != nil {
		// Pipeline overall error, record
		errs = append(errs, err)
	}

	// Log success for individual keys (optional, can be removed in production for speed)
	for _, data := range batch {
		if data.Expiration > 0 {
			fmt.Printf("Set expiration for key %s to %d seconds\n", data.Key, data.Expiration)
		} else {
			fmt.Printf("Key %s set to never expire\n", data.Key)
		}
	}

	processed = len(batch) - len(errs)
	failed := len(errs)
	for _, e := range errs {
		log.Printf("Import error: %v", e)
	}

	return processed, failed, skipped
}

// Add a helper function to check key TTL
func checkKeyTTL(rdb redis.UniversalClient, key string) (time.Duration, error) {
	return rdb.TTL(ctx, key).Result()
}

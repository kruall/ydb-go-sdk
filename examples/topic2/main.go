package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path"
	"strings"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	sdklog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var connectionString = flag.String("ydb", "grpc://localhost:2136/local", "")

type logFlag struct {
	enabled bool
	level   sdklog.Level
}

func (f *logFlag) String() string {
	if !f.enabled {
		return ""
	}
	return strings.ToLower(f.level.String())
}

func (f *logFlag) Set(s string) error {
	f.enabled = true
	if s == "" || s == "true" {
		f.level = sdklog.DEBUG
		return nil
	}
	if s == "false" {
		f.enabled = false
		return nil
	}
	switch strings.ToLower(s) {
	case "trace":
		f.level = sdklog.TRACE
	case "debug":
		f.level = sdklog.DEBUG
	case "info":
		f.level = sdklog.INFO
	case "warn":
		f.level = sdklog.WARN
	case "error":
		f.level = sdklog.ERROR
	default:
		return fmt.Errorf("invalid log level: %s", s)
	}
	return nil
}

func (f *logFlag) IsBoolFlag() bool { return true }

var driverLog logFlag

func init() {
	flag.Var(&driverLog, "driver-log", "enable driver debug logging (optional level: trace|debug|info|warn|error)")
}

func main() {
	flag.Parse()

	// Use 5-second timeout for connection as specified in requirements
	// Local YDB instances typically respond quickly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := make([]ydb.Option, 0)
	if driverLog.enabled {
		opts = append(opts,
			ydb.WithLogger(
				sdklog.Default(os.Stderr,
					sdklog.WithColoring(),
					sdklog.WithMinLevel(driverLog.level),
				),
				trace.DetailsAll,
				sdklog.WithLogQuery(),
			),
		)
	}

	// Connect to YDB
	db, err := ydb.Open(ctx, *connectionString, opts...)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	// Construct topic path using established pattern
	topicPath := path.Join(db.Name(), "example-topic")

	// Step 1: Delete topic if exists (ignore schema errors)
	stdlog.Println("Deleting topic (if exists)...")
	err = db.Query().Exec(ctx, `DROP TOPIC IF EXISTS `+"`"+topicPath+"`")
	if err != nil {
		panic(fmt.Errorf("drop topic error: %w", err))
	}
	stdlog.Println("Topic deleted (if existed)")

	// Step 2: Create topic via YQL
	stdlog.Println("Creating topic...")
	err = db.Query().Exec(ctx, `CREATE TOPIC `+"`"+topicPath+"`"+` (
		CONSUMER consumer1
	)`)
	if err != nil {
		panic(fmt.Errorf("create topic error: %w", err))
	}
	stdlog.Println("Topic created successfully")

	// Step 3: Write 3 messages to the topic
	stdlog.Println("Writing 3 messages...")
	writer, err := db.Topic().StartWriter(topicPath)
	if err != nil {
		panic(fmt.Errorf("start writer error: %w", err))
	}
	defer func() { _ = writer.Close(ctx) }()

	// Write 3 messages with different content
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for i, content := range messages {
		message := topicwriter.Message{
			Data: bytes.NewReader([]byte(content)),
		}
		err = writer.Write(ctx, message)
		if err != nil {
			panic(fmt.Errorf("write message %d error: %w", i+1, err))
		}
		stdlog.Printf("Message %d written successfully", i+1)
	}

	// Step 4: Read messages in batches from the topic
	stdlog.Println("Starting batch reader...")
	reader, err := db.Topic().StartReader("consumer1",
		topicoptions.ReadTopic(topicPath),
	)
	if err != nil {
		panic(fmt.Errorf("start reader error: %w", err))
	}
	defer func() { _ = reader.Close(ctx) }()

	// Read messages in batches with 1-second timeout
	totalMessagesRead := 0

	for {
		// Create context with 1-second timeout for each read operation
		readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)

		batch, err := reader.ReadMessagesBatch(readCtx)
		readCancel()

		if err != nil {
			if readCtx.Err() == context.DeadlineExceeded {
				stdlog.Println("Read timeout reached, no more messages available")
				break
			}
			panic(fmt.Errorf("read batch error: %w", err))
		}

		if batch == nil || len(batch.Messages) == 0 {
			stdlog.Println("No messages in batch, continuing...")
			continue
		}

		// Process messages in the batch
		for _, msg := range batch.Messages {
			content, err := io.ReadAll(msg)
			if err != nil {
				panic(fmt.Errorf("read message content error: %w", err))
			}

			stdlog.Printf("Message read: %s", string(content))
			stdlog.Printf("Offset: %d", msg.Offset)
			totalMessagesRead++
		}

		stdlog.Printf("Batch processed with %d messages", len(batch.Messages))

		// Commit the batch immediately after processing
		err = reader.Commit(batch.Context(), batch)
		if err != nil {
			panic(fmt.Errorf("commit batch error: %w", err))
		}
		stdlog.Printf("Batch committed successfully")
	}

	stdlog.Printf("Example completed successfully - read %d messages total", totalMessagesRead)
}

package inmem

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// WAL is a write-ahead log. Records are fsynced to disk before the
// corresponding in-memory state mutation is applied, so a crash between
// write and mutation leaves the record on disk and the mutation un-applied —
// the correct state for replay.
//
// File mode 0600: payload data may contain PII or tokens.
type WAL struct {
	file *os.File
}

// WALRecord is one line in the WAL file. Exported so queue.go can apply
// broker semantics without importing a separate type.
type WALRecord struct {
	Type      string    `json:"type"`
	MessageID string    `json:"messageID"`
	Payload   []byte    `json:"payload,omitempty"` // base64-encoded by encoding/json; present only in publish records
	Timestamp time.Time `json:"ts"`
}

// OpenWAL opens (or creates) the WAL file at path in append-only mode.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: f}, nil
}

func (w *WAL) appendPublish(messageID string, payload []byte) error {
	return w.write(WALRecord{
		Type:      "publish",
		MessageID: messageID,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	})
}

func (w *WAL) appendAck(messageID string) error {
	return w.write(WALRecord{
		Type:      "ack",
		MessageID: messageID,
		Timestamp: time.Now().UTC(),
	})
}

// write marshals r as a single JSON line and fsyncs before returning.
func (w *WAL) write(r WALRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("wal marshal: %w", err)
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// ReplayWAL opens the WAL at path read-only and returns all records in order.
// Returns nil, nil if the file does not exist (clean first run).
// Returns an error on any JSON parse failure — corrupt tails are a later concern.
func ReplayWAL(path string) ([]WALRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("replay wal open: %w", err)
	}
	defer f.Close()

	var records []WALRecord
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var r WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("wal corrupt at line %d: %w", lineNum, err)
		}
		records = append(records, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("wal scan: %w", err)
	}
	return records, nil
}

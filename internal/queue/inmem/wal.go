package inmem

import (
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

type walRecord struct {
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
	return w.write(walRecord{
		Type:      "publish",
		MessageID: messageID,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	})
}

func (w *WAL) appendAck(messageID string) error {
	return w.write(walRecord{
		Type:      "ack",
		MessageID: messageID,
		Timestamp: time.Now().UTC(),
	})
}

// write marshals r as a single JSON line and fsyncs before returning.
func (w *WAL) write(r walRecord) error {
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

package integration

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type MAC [32]byte

var (
	NilMac MAC
)

func (m MAC) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.FormatHex())
}

func (m *MAC) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	return m.ParseHex(s)
}

func (m MAC) FormatHex() string {
	return hex.EncodeToString(m[:])
}

func (m *MAC) ParseHex(s string) error {
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return err
	}

	if len(decoded) != 32 {
		return fmt.Errorf("invalid mac length: %d", len(decoded))
	}

	copy(m[:], decoded)
	return nil
}

func MACFromBytes(b []byte) (MAC, error) {
	if len(b) != len(MAC{}) {
		return MAC{}, fmt.Errorf("invalid MAC length: %d", len(b))
	}

	return MAC(b), nil
}

func RandomMAC() MAC {
	var r MAC

	// According to the documentation, this call never fails and
	// always return a complete read.
	rand.Read(r[:])

	return r
}

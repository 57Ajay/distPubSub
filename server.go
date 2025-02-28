package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu         sync.Mutex
	data       map[string]string
	expiration map[string]time.Time
}

func NewStore() *Store {
	store := &Store{
		data:       make(map[string]string),
		expiration: make(map[string]time.Time),
	}
	go store.cleanUpExpiryKeys()
	return store
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key, value string, TTLSeconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	if TTLSeconds > 0 {
		s.expiration[key] = time.Now().Add(time.Duration(TTLSeconds) * time.Second)
	} else {
		delete(s.expiration, key)
	}
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.expiration, key)
}

func (s *Store) cleanUpExpiryKeys() {
	for {
		time.Sleep(1 * time.Second)
		s.mu.Lock()
		now := time.Now()
		for key, expiry := range s.expiration {
			if now.After(expiry) {
				delete(s.data, key)
				delete(s.expiration, key)
			}
		}
		s.mu.Unlock()
	}
}

func handleConnections(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		// Try to parse as RESP protocol first
		req, err := parseRESP(reader)
		if err != nil {
			// If RESP parsing fails, try parsing as plain text
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Client disconnected:", err)
				return
			}

			// Parse plain text command
			line = strings.TrimSpace(line)
			if line == "" {
				conn.Write([]byte("ERROR: Empty command\n"))
				continue
			}

			req = strings.Fields(line)
		}

		if len(req) == 0 {
			conn.Write([]byte("ERROR: Empty command\n"))
			continue
		}

		command := strings.ToUpper(req[0])

		switch command {
		case "SET":
			{
				if len(req) < 3 {
					conn.Write([]byte("ERROR: SET command requires at least key and value arguments\n"))
					continue
				}
				ttl := 0
				if len(req) >= 5 && strings.ToUpper(req[3]) == "EX" {
					fmt.Sscanf(req[4], "%d", &ttl)
				}
				store.Set(req[1], req[2], ttl)
				conn.Write([]byte("OK\n"))
			}
		case "GET":
			{
				if len(req) != 2 {
					conn.Write([]byte("ERROR: GET command requires exactly one key argument\n"))
					continue
				}
				if val, ok := store.Get(req[1]); ok {
					conn.Write([]byte(val + "\n"))
				} else {
					conn.Write([]byte("nil\n"))
				}
			}
		case "DEL":
			{
				if len(req) != 2 {
					conn.Write([]byte("ERROR: DEL command requires exactly one key argument\n"))
					continue
				}
				DeleteKey := req[1]
				store.Delete(DeleteKey)
				conn.Write([]byte("OK\n"))
			}
		default:
			{
				conn.Write([]byte("ERROR: Invalid command\n"))
			}
		}
	}
}

func acceptConnections(listener net.Listener, store *Store) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection")
			return
		}
		fmt.Println("*****Client connected*****")
		go handleConnections(conn, store)
	}
}

func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Peek at the first byte to see if it's a RESP array
	b, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}

	// If it's not a RESP array, return an error to trigger plain text parsing
	if b[0] != '*' {
		return nil, fmt.Errorf("not RESP format")
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	count := 0
	_, err = fmt.Sscanf(line, "*%d", &count)
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %v", err)
	}

	args := make([]string, count)
	for i := range args {
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		strLen := 0
		_, err = fmt.Sscanf(lenLine, "$%d", &strLen)
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %v", err)
		}

		data := make([]byte, strLen+2) // +2 for \r\n
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}

		if string(data[strLen:]) != "\r\n" {
			return nil, fmt.Errorf("missing CRLF")
		}

		args[i] = string(data[:strLen])
	}
	return args, nil
}

func main() {
	store := NewStore()
	listener, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	fmt.Println("Server is running on localhost:6379")
	defer listener.Close()
	go acceptConnections(listener, store)
	select {}
}

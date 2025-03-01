package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu         sync.RWMutex
	data       map[string]string
	expiration map[string]time.Time

	// PubSub
	subscribers map[string]map[net.Conn]bool // {channel:{conn1:true, conn2:true}} EX: {key: {192.8.7.69: true}}
	pubSubMu    sync.RWMutex

	//AOF
	aofFile *os.File // This is gonna be Append Only File
}

func NewStore() *Store {

	aof, err := os.OpenFile("appendonly.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error opening aof File", err)
	}

	store := &Store{
		data:       make(map[string]string),
		expiration: make(map[string]time.Time),

		subscribers: make(map[string]map[net.Conn]bool),

		aofFile: aof,
	}

	store.LoadAOF()

	go store.cleanUpExpiryKeys()
	return store
}

func (s *Store) LoadAOF() {
	file, err := os.Open("appendonly.aof")

	if err != nil {
		if os.IsNotExist(err) {
			return // this means no aif file yet
		}
		log.Fatal("Error opening/loading aof file", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), " ")
		if len(parts) < 2 {
			continue
		}
		command := strings.ToUpper(parts[0])
		switch command {
		case "SET":
			{
				if len(parts) < 3 {
					continue
				}
				key, value := parts[1], parts[2]
				ttl := 0
				if len(parts) == 4 {
					fmt.Sscanf(parts[3], "%d", &ttl)
				}
				s.data[key] = value
				if ttl > 0 {
					s.expiration[key] = time.Now().Add(time.Duration(ttl) * time.Second)
				}
			}
		case "DEL":
			{
				if len(parts) < 2 {
					continue
				}
				key := parts[1]
				delete(s.data, key)
			}
		}
	}
}

func (s *Store) Subscribe(key string, conn net.Conn) {
	s.pubSubMu.Lock()
	defer s.pubSubMu.Unlock()
	if _, ok := s.subscribers[key]; !ok {
		s.subscribers[key] = make(map[net.Conn]bool)
	}
	s.subscribers[key][conn] = true
}

func (s *Store) Unsubscribe(key string, conn net.Conn) {
	s.pubSubMu.Lock()
	defer s.pubSubMu.Unlock()
	if _, ok := s.subscribers[key]; ok {
		delete(s.subscribers[key], conn)
		if len(s.subscribers[key]) == 0 {
			delete(s.subscribers, key)
		}
	}
}

func (s *Store) Publish(channel, message string) {
	s.pubSubMu.RLock()
	defer s.pubSubMu.RUnlock()

	for conn := range s.subscribers[channel] {
		var buf bytes.Buffer
		buf.WriteString("*3\r\n$7\r\nmessage\r\n")
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(channel), channel)
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(message), message)

		// Debugging line changed to avoid corrupting the RESP protocol (this caused so mush error)
		// conn.Write([]byte(fmt.Sprintf("\n\nMessage received: %s\n\n", message)))

		// Just write the RESP response directly
		conn.Write(buf.Bytes())
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	if s.aofFile != nil {
		_, _ = s.aofFile.WriteString(fmt.Sprintf("SET %s %s\n", key, value))
		s.aofFile.Sync()
	}
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.expiration, key)

	if s.aofFile != nil {
		_, _ = s.aofFile.WriteString(fmt.Sprintf("DEL %s\n", key))
		s.aofFile.Sync()
	}
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
		// Trying to parse as RESP protocol first
		req, err := parseRESP(reader)
		if err != nil {
			// If RESP parsing fails, parsing as plain text
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Client disconnected:", err)
				return
			}

			// Parseing plain text command
			line = strings.TrimSpace(line)
			if line == "" {
				conn.Write([]byte("ERROR: Empty command\n"))
				continue
			}

			req = parseCommand(line)
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
				conn.Write([]byte(":1\r\n"))
			}
		case "SUBSCRIBE":
			{
				if len(req) < 2 {
					conn.Write([]byte("ERROR: SUBSCRIBE command requires at least one channel argument\n"))
					continue
				}
				channel := req[1]
				store.Subscribe(channel, conn)
				var buf bytes.Buffer
				buf.WriteString("Subscribed to ")
				fmt.Fprintf(&buf, "%s\n", channel)
				conn.Write(buf.Bytes())
			}
		case "UNSUBSCRIBE":
			{
				if len(req) < 2 {
					conn.Write([]byte("ERROR: UNSUBSCRIBE command requires at least one channel argument\n"))
					continue
				}
				channel := req[1]
				store.Unsubscribe(channel, conn)
				var buf bytes.Buffer
				buf.WriteString("Unsubscribed from ")
				fmt.Fprintf(&buf, "%s\n", channel)
				conn.Write(buf.Bytes())
			}
		case "PUBLISH":
			{
				if len(req) < 3 {
					conn.Write([]byte("ERROR: PUBLISH requires channel and message\n"))
					continue
				}
				channel := req[1]

				// For plain text mode, join all the remaining arguments as the message
				message := strings.Join(req[2:], " ")

				// If the message is quoted, remove the quotes
				if len(message) > 1 && message[0] == '"' && message[len(message)-1] == '"' {
					message = message[1 : len(message)-1]
				}

				store.Publish(channel, message)
				count := 0
				store.pubSubMu.RLock()
				if subs, ok := store.subscribers[channel]; ok {
					count = len(subs)
				}
				store.pubSubMu.RUnlock()

				var buf bytes.Buffer
				buf.WriteString("Published to ")
				fmt.Fprintf(&buf, "%d subscribers\n", count)
				conn.Write(buf.Bytes())
			}
		default:
			{
				conn.Write([]byte("ERROR: Invalid command\n"))
			}
		}
	}
}

func parseCommand(line string) []string {
	var result []string
	var current bytes.Buffer
	inQuotes := false

	for i := range len(line) {
		c := line[i]

		if c == '"' {
			inQuotes = !inQuotes
			continue
		}

		if c == ' ' && !inQuotes {
			if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteByte(c)
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func acceptConnections(listener net.Listener, store *Store) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
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

		if len(data) < 2 || string(data[strLen:]) != "\r\n" {
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
	acceptConnections(listener, store)
}

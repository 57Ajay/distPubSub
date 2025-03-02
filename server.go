package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu         sync.RWMutex
	data       map[string]string
	expiration map[string]time.Time

	subscribers map[string]map[net.Conn]bool
	pubSubMu    sync.RWMutex

	aofFile   *os.File
	aofBuffer bytes.Buffer
	aofMu     sync.Mutex
}

func NewStore() *Store {
	aof, err := os.OpenFile("appendonly.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("AOF error:", err)
	}

	store := &Store{
		data:        make(map[string]string),
		expiration:  make(map[string]time.Time),
		subscribers: make(map[string]map[net.Conn]bool),
		aofFile:     aof,
	}

	store.LoadAOF()
	go store.aofFlushWorker()
	go store.rewriteAOF()
	go store.cleanUpExpiryKeys()
	return store
}

func (s *Store) aofFlushWorker() {
	for {
		time.Sleep(2 * time.Second)
		s.aofMu.Lock()
		if s.aofBuffer.Len() > 0 {
			if _, err := s.aofFile.Write(s.aofBuffer.Bytes()); err != nil {
				log.Println("AOF write error:", err)
			}
			s.aofFile.Sync()
			s.aofBuffer.Reset()
		}
		s.aofMu.Unlock()
	}
}

func (s *Store) rewriteAOF() {
	for {
		time.Sleep(10 * time.Minute)
		s.mu.Lock()

		tempFile, err := os.CreateTemp("", "aof-rewrite")
		if err != nil {
			log.Println("AOF rewrite error:", err)
			s.mu.Unlock()
			continue
		}

		for key, value := range s.data {
			if expiry, ok := s.expiration[key]; ok && time.Now().After(expiry) {
				continue
			}

			ttl := 0
			if expiry, ok := s.expiration[key]; ok {
				ttl = int(time.Until(expiry).Seconds())
				if ttl < 0 {
					continue
				}
			}

			var command string
			if ttl > 0 {
				command = fmt.Sprintf("*4\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$2\r\nEX\r\n$%d\r\n%d\r\n",
					len(key), key, len(value), value, len(fmt.Sprint(ttl)), ttl)
			} else {
				command = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(key), key, len(value), value)
			}
			if _, err := tempFile.WriteString(command); err != nil {
				log.Println("AOF rewrite write error:", err)
			}
		}

		tempFile.Close()
		if err := os.Rename(tempFile.Name(), "appendonly.aof"); err != nil {
			log.Println("AOF rename error:", err)
			s.mu.Unlock()
			continue
		}

		s.aofMu.Lock()
		s.aofFile.Close()
		s.aofFile, err = os.OpenFile("appendonly.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("AOF reopen error:", err)
		}
		s.aofMu.Unlock()
		s.mu.Unlock()
	}
}

func (s *Store) LoadAOF() {
	file, err := os.Open("appendonly.aof")
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Fatal("AOF load error:", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		args, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		switch command {
		case "SET":
			if len(args) < 3 {
				continue
			}
			key := args[1]
			value := args[2]
			ttl := 0
			if len(args) >= 5 && strings.ToUpper(args[3]) == "EX" {
				fmt.Sscanf(args[4], "%d", &ttl)
			}
			s.Set(key, value, ttl)
		case "DEL":
			if len(args) < 2 {
				continue
			}
			s.Delete(args[1])
		}
	}
}

func (s *Store) cleanUpExpiryKeys() {
	for {
		time.Sleep(1 * time.Second)
		now := time.Now()
		var expired []string

		s.mu.RLock()
		for key, expiry := range s.expiration {
			if now.After(expiry) {
				expired = append(expired, key)
			}
		}
		s.mu.RUnlock()

		for _, key := range expired {
			s.Delete(key)
		}
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if expiry, ok := s.expiration[key]; ok && time.Now().After(expiry) {
		return "", false
	}
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key, value string, ttl int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	if ttl > 0 {
		s.expiration[key] = time.Now().Add(time.Duration(ttl) * time.Second)
	} else {
		delete(s.expiration, key)
	}

	var command string
	if ttl > 0 {
		command = fmt.Sprintf("*4\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$2\r\nEX\r\n$%d\r\n%d\r\n",
			len(key), key, len(value), value, len(fmt.Sprint(ttl)), ttl)
	} else {
		command = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(key), key, len(value), value)
	}

	s.aofMu.Lock()
	s.aofBuffer.WriteString(command)
	s.aofMu.Unlock()
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.expiration, key)

	command := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
	s.aofMu.Lock()
	s.aofBuffer.WriteString(command)
	s.aofMu.Unlock()
}

func (s *Store) Subscribe(channel string, conn net.Conn) {
	s.pubSubMu.Lock()
	defer s.pubSubMu.Unlock()
	if _, ok := s.subscribers[channel]; !ok {
		s.subscribers[channel] = make(map[net.Conn]bool)
	}
	s.subscribers[channel][conn] = true
}

func (s *Store) Unsubscribe(channel string, conn net.Conn) {
	s.pubSubMu.Lock()
	defer s.pubSubMu.Unlock()
	if subs, ok := s.subscribers[channel]; ok {
		delete(subs, conn)
		if len(subs) == 0 {
			delete(s.subscribers, channel)
		}
	}
}

func (s *Store) Publish(channel, message string) int {
	s.pubSubMu.RLock()
	defer s.pubSubMu.RUnlock()
	count := 0

	for conn := range s.subscribers[channel] {
		resp := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(channel), channel, len(message), message)
		if _, err := conn.Write([]byte(resp)); err != nil {
			s.Unsubscribe(channel, conn)
		} else {
			count++
		}
	}
	return count
}

func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	defer func() {
		store.pubSubMu.Lock()
		for channel := range store.subscribers {
			delete(store.subscribers[channel], conn)
		}
		store.pubSubMu.Unlock()
	}()

	for {
		args, err := parseRESP(reader)
		if err != nil {
			if err != io.EOF {
				log.Println("Parse error:", err)
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		switch command {
		case "SET":
			if len(args) < 3 {
				writeError(conn, "ERR wrong number of arguments for 'set' command")
				continue
			}
			ttl := 0
			if len(args) >= 5 && strings.ToUpper(args[3]) == "EX" {
				ttl, _ = strconv.Atoi(args[4])
			}
			store.Set(args[1], args[2], ttl)
			writeSimpleString(conn, "OK")

		case "GET":
			if len(args) != 2 {
				writeError(conn, "ERR wrong number of arguments for 'get' command")
				continue
			}
			if val, ok := store.Get(args[1]); ok {
				writeBulkString(conn, val)
			} else {
				writeNull(conn)
			}

		case "DEL":
			if len(args) != 2 {
				writeError(conn, "ERR wrong number of arguments for 'del' command")
				continue
			}
			store.Delete(args[1])
			writeInteger(conn, 1)

		case "SUBSCRIBE":
			if len(args) < 2 {
				writeError(conn, "ERR wrong number of arguments for 'subscribe' command")
				continue
			}
			for _, channel := range args[1:] {
				store.Subscribe(channel, conn)
				writeArray(conn, 3)
				writeBulkString(conn, "subscribe")
				writeBulkString(conn, channel)
				writeInteger(conn, 1)
			}

		case "UNSUBSCRIBE":
			if len(args) < 2 {
				writeError(conn, "ERR wrong number of arguments for 'unsubscribe' command")
				continue
			}
			for _, channel := range args[1:] {
				store.Unsubscribe(channel, conn)
				writeArray(conn, 3)
				writeBulkString(conn, "unsubscribe")
				writeBulkString(conn, channel)
				writeInteger(conn, 0)
			}

		case "PUBLISH":
			if len(args) < 3 {
				writeError(conn, "ERR wrong number of arguments for 'publish' command")
				continue
			}
			count := store.Publish(args[1], strings.Join(args[2:], " "))
			writeInteger(conn, count)

		default:
			writeError(conn, "ERR unknown command '"+command+"'")
		}
	}
}

// RESP protocol writers
func writeSimpleString(conn net.Conn, s string) {
	conn.Write([]byte("+" + s + "\r\n"))
}

func writeError(conn net.Conn, msg string) {
	conn.Write([]byte("-" + msg + "\r\n"))
}

func writeInteger(conn net.Conn, n int) {
	conn.Write([]byte(":" + strconv.Itoa(n) + "\r\n"))
}

func writeBulkString(conn net.Conn, s string) {
	if s == "" {
		conn.Write([]byte("$-1\r\n"))
	} else {
		conn.Write([]byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"))
	}
}

func writeNull(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

func writeArray(conn net.Conn, length int) {
	conn.Write([]byte("*" + strconv.Itoa(length) + "\r\n"))
}

// RESP parser
func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '*' {
		return parseRESPArray(reader, line)
	}

	return parseInlineCommand(line)
}

func parseRESPArray(reader *bufio.Reader, header string) ([]string, error) {
	size, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid array size")
	}

	args := make([]string, size)
	for i := range size {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		strSize, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || strSize < 0 {
			return nil, fmt.Errorf("invalid bulk string size")
		}

		buf := make([]byte, strSize+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		args[i] = string(buf[:strSize])
	}
	return args, nil
}

func parseInlineCommand(line string) ([]string, error) {
	line = strings.TrimRight(line, "\r\n")
	var args []string
	var buf []byte
	inQuote := false

	for _, c := range line {
		switch {
		case c == '"':
			inQuote = !inQuote
		case !inQuote && c == ' ':
			if len(buf) > 0 {
				args = append(args, string(buf))
				buf = nil
			}
		default:
			buf = append(buf, byte(c))
		}
	}

	if len(buf) > 0 {
		args = append(args, string(buf))
	}

	if inQuote {
		return nil, fmt.Errorf("unbalanced quotes")
	}

	return args, nil
}

func main() {
	store := NewStore()
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("Failed to start server:", err)
	}
	defer listener.Close()

	log.Println("Server listening on port 6379")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go handleConnection(conn, store)
	}
}

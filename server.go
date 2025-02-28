package main

import (
	"bufio"
	"fmt"
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
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err)
			log.Println("*****Client disconnected*****")
			return
		}
		input = strings.TrimSpace(input)
		parts := strings.Split(input, " ")

		if len(parts) < 1 {
			fmt.Println("Invalid input")
			continue
		}

		commands := strings.ToUpper(parts[0])
		switch commands {
		case "SET":
			{
				if len(parts) < 3 {
					fmt.Println("Invalid input, SET command should have 2 arguments a value and a key and optional TTL")
					conn.Write([]byte("Invalid input, SET command should have 2 arguments a value and a key and optional TTl\n"))
					continue
				}
				ttl := 0
				if len(parts) == 5 && strings.ToUpper(parts[3]) == "EX" {
					fmt.Sscanf(parts[4], "%d", &ttl)
				}
				store.Set(parts[1], parts[2], ttl)
				conn.Write([]byte("OK\n"))
			}
		case "GET":
			{
				if len(parts) < 2 || len(parts) > 2 {
					fmt.Println("Invalid input, GET command should have 1 argument a key")
					conn.Write([]byte("Invalid input, GET command should have 1 argument a key\n"))
					continue
				}
				if val, ok := store.Get(parts[1]); ok {
					conn.Write([]byte(val + "\n"))
				} else {
					conn.Write([]byte("nil\n"))
				}
			}
		case "DEL":
			{
				if len(parts) < 2 || len(parts) > 2 {
					conn.Write([]byte("Invalid input, DEL command should have 1 argument a key\n"))
					continue
				}
				DeleteKey := parts[1]
				store.Delete(DeleteKey)
				conn.Write([]byte("OK\n"))
			}
		default:
			{
				conn.Write([]byte("Invalid command\n"))
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

func main() {
	store := NewStore()
	listener, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Error listening:", err)
		log.Println("*****Client disconnected*****")
	}
	fmt.Println("Server is running on localhost:6379")
	defer listener.Close()
	go acceptConnections(listener, store)
	select {}
}

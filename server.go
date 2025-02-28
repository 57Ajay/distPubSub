package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type Store struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func handleConnections(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err)
			log.Fatal("*****Client disconnected*****")
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
				if len(parts) < 3 || len(parts) > 3 {
					fmt.Println("Invalid input, SET command should have 2 arguments a value and a key")
					conn.Write([]byte("Invalid input, SET command should have 2 arguments a value and a key\n"))
					continue
				}
				store.Set(parts[1], parts[2])
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
			fmt.Println("Error accepting connection")
			log.Fatal("*****Client disconnected*****")
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
		log.Fatal("*****Client disconnected*****")
	}
	fmt.Println("Server is running on localhost:6379")
	defer listener.Close()
	go acceptConnections(listener, store)
	select {}
}

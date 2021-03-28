package main

import (
	"collector/pkg/entities"
	"fmt"
	"net"
	"sync"
	"time"
)

func (cp *CollectingProcess) startUDPServer() {
	var err error
	var conn net.Conn
	var wg sync.WaitGroup
	address, err := net.ResolveUDPAddr(cp.protocol, cp.address)
	if err != nil {
		LOGGER.Println(err)
		return
	}
	conn, err = net.ListenUDP("udp", address)
	if err != nil {
		LOGGER.Println(err)
		return
	}
	cp.updateAddress(conn.LocalAddr())
	fmt.Printf("Start UDP collecting process on %s", cp.address)
	defer conn.Close()
	go func() {
		for {
			buff := make([]byte, cp.maxBufferSize)
			size, err := conn.Read(buff)
			if err != nil {
				LOGGER.Printf("Error in udp collecting process: %v, err")
				return
			}
			LOGGER.Printf("$$$ Receiving %d bytes from %s", size, address)
			cp.handleUDPClient(address, &wg)
		}
	}()
	<-cp.stopChan
	cp.closeAllClients()
	wg.Wait()
}


func (cp *CollectingProcess) handleUDPClient(address net.Addr, wg *sync.WaitGroup) {
	if _, exist := cp.clients[address.String()]; !exist {
		client := cp.createClient()
		cp.addClient(address.String(), client)
		wg.Add(1)
		defer wg.Done()
		go func() {
			ticker := time.NewTicker(time.Duration(entities.TemplateRefreshTimeOut) * time.Second)
			for {
				select {
				case <-client.errChan:

					return
				case <-ticker.C: // set timeout for client connection

					cp.deleteClient(address.String())
					return
				case packet := <-client.packetChan:
					// get the message here
					message, err := cp.decodePacket(packet, address.String())
					if err != nil {

						return
					}
					fmt.Print("$$$$$$ the message:", message)
					ticker.Stop()
					ticker = time.NewTicker(time.Duration(entities.TemplateRefreshTimeOut) * time.Second)
				}
			}
		}()
	}
}

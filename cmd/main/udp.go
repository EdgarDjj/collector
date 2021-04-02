package main

import (
	"bytes"
	"collector/pkg/entities"
	"log"
	"net"
	"sync"
	"time"
)

func (cp *CollectingProcess) startUDPServer() {
	var wg sync.WaitGroup
	udpAddress, err := net.ResolveUDPAddr(cp.protocol, cp.address)
	if err != nil {
		log.Println(err)
		return
	}
	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Println(err)
		return
	}

	cp.updateAddress(conn.LocalAddr())
	log.Printf("Start UDP collecting process on %s\n", cp.address)

	defer conn.Close()
	go func() {
		for {
			buff := make([]byte, cp.maxBufferSize)
			size, address, err := conn.ReadFromUDP(buff)

			if err != nil {
				if size == 0 {
					return
				}
				log.Printf("Error in udp collecting process: %v\n", err)
				return
			}
			log.Printf("$$$ Receiving %d bytes from %s\n", size, address.String())
			cp.handleUDPClient(address, &wg)
			cp.clients[address.String()].packetChan <- bytes.NewBuffer(buff[0:size])
		}
	}()
	<-cp.stopChan
	cp.closeAllClients()
	wg.Wait()
}

func (cp *CollectingProcess) handleUDPClient(address net.Addr, wg *sync.WaitGroup) {
	log.Println("$$$ handlesUDPClient start : the address :" + address.String())
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
					log.Println("some error in Collecting process")
					return
				case <-ticker.C: // set timeout for client connection
					log.Printf("delete Client() UDP connection from %s timed out.\n", address.String())
					cp.deleteClient(address.String())
					return
				case packet := <-client.packetChan:
					// get the message here
					log.Println("$$$ start decodePacket")
					message, err := cp.decodePacket(packet, address.String())
					if err != nil {
						return
					}
					log.Println("$$$ the decodePacket: ", message)
					ticker.Stop()
					ticker = time.NewTicker(time.Duration(entities.TemplateRefreshTimeOut) * time.Second)
				}
			}
		}()
	}
}

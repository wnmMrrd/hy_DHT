package main

import (
	"chord"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

type DHTNode struct {
	nd *chord.RPCNode
	ser *rpc.Server
}

func NewNode(port int) dhtNode {
	var p DHTNode
	p.nd=new(chord.RPCNode)
	p.nd.Nd=new(chord.Node)
	p.nd.Nd.Init(":"+strconv.Itoa(port))
	p.ser=rpc.NewServer()
	err:=p.ser.Register(p.nd)
	if err!=nil {
		fmt.Println("register fail")
		panic(nil)
	}
	return &p
}

func (p *DHTNode) Run() {
	listen,err:=net.Listen("tcp",p.nd.Nd.Adr)
	if err!=nil {
		fmt.Println(err)
		fmt.Println("listen failed")
		return
	}
	p.nd.Listen=listen
	p.nd.Nd.On=true
	go p.ser.Accept(listen)
	go p.nd.Nd.Maintain()
}

func (p *DHTNode) Create() {
	p.nd.Nd.Create()
}

func (p *DHTNode) Join(adr string) bool {
	ans:=p.nd.Nd.Join(adr)==nil
	return ans
}

func (p *DHTNode) Quit() {
	if p.nd.Nd.On==false {
		return
	}
	p.nd.Nd.On=false
	_=p.nd.Nd.Quit()
	err:=p.nd.Listen.Close()
	if err!=nil {
		fmt.Println("Quit Fail")
	}
}

func (p *DHTNode) ForceQuit() {
	if p.nd.Nd.On==false {
		return
	}
	p.nd.Nd.On=false
	err:=p.nd.Listen.Close()
	if err!=nil {
		fmt.Println("Quit Fail")
	}
}

func (p *DHTNode) Ping(adr string) bool {
	return chord.Ping(adr)==nil
}

func (p *DHTNode) Put(key string,val string) bool {
	return p.nd.Nd.Insert(key,val)==nil
}

func (p *DHTNode) Get(key string) (bool,string) {
	var ans string
	err:=p.nd.Nd.Queryall(key,&ans)
	return err==nil,ans
}

func (p *DHTNode) Delete(key string) bool {
	return p.nd.Nd.Eraseall(key)==nil
}
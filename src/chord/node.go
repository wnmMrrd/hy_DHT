package chord

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)

const (
	M=160
	sucl=30
	waitT=50*time.Millisecond
)
//var Flag bool
type KVmap struct{
	mp map[string]string
	lock sync.Mutex
}

type Edge struct{
	Adr string
	Id big.Int
}

type Node struct{
	Adr string
	id big.Int
	pre Edge
	finger [M]Edge
	suc [sucl]Edge
	lock sync.Mutex
	dt KVmap
	predt KVmap
	On bool
	innet bool
}

type Keyval struct{
	Key, Val string
}

func (p *Node) Init(address string) {
	p.Adr=address
	p.id=*Hash(address)
	p.dt.mp=make(map[string]string)
	p.predt.mp=make(map[string]string)
	p.innet=false
}

func (p *Node) Replacepredt(mp map[string]string) {
	p.predt.lock.Lock()
	p.predt.mp=mp
	p.predt.lock.Unlock()
}

func (p *Node) Notify(Pre Edge) error {
	if Ping(p.pre.Adr)!=nil||Incheck(&p.pre.Id,&p.id,&Pre.Id) {
		p.lock.Lock()
		p.pre=Pre
		p.lock.Unlock()
		client := Dial(Pre.Adr)
		if client == nil {
			fmt.Println("notify1")
			return errors.New("notify1")
		}
		mp:=make(map[string]string)
		err := client.Call("RPCNode.Getdt", 0,&mp)
		_ = client.Close()
		if err != nil {
			fmt.Println("notify2")
			return errors.New("notify2")
		}
		p.Replacepredt(mp)
	}
	return nil
}

func (p *Node) Fixsuc() error {
	p.lock.Lock()
	var index int
	for index=0;index<sucl;index++ {
		if Ping(p.suc[index].Adr)==nil {
			break
		}
	}
	if index==sucl {
		fmt.Println("fixsuc1")
		return errors.New("fixsuc1")
	} else if index==0 {
		p.lock.Unlock()
	} else {
		ed:=p.suc[index]
		for i:=index;i<sucl;i++ {
			p.suc[i-index]=p.suc[i]
		}
		p.lock.Unlock()
		time.Sleep(waitT*3/2)
		client:=Dial(ed.Adr)
		if client==nil {
			fmt.Println("fixsuc2")
			return errors.New("fixsuc2")
		}
		err:=client.Call("RPCNode.Notify",&Edge{p.Adr,p.id},nil)
		_=client.Close()
		if err!=nil {
			fmt.Println("fixsuc3")
			return errors.New("fixscu3")
		}
	}
	return nil
}

func (p *Node) Addlink(Nxt Edge) error {
	p.lock.Lock()
	if p.suc[0].Adr==Nxt.Adr {
		p.lock.Unlock()
		return nil
	}
	for i:=sucl-1;i>0;i-- {
		p.suc[i]=p.suc[i-1]
	}
	p.suc[0]=Nxt
	p.lock.Unlock()
	client:=Dial(Nxt.Adr)
	if client == nil {
		fmt.Println("link fail1")
		return errors.New("link fail1")
	}
	p.dt.lock.Lock()
	mp:=p.dt.mp
	p.dt.lock.Unlock()
	err:=client.Call("RPCNode.Replacepredt",mp,nil)
	_=client.Close()
	if err!=nil {
		fmt.Println("link fail2")
		return errors.New("link fail2")
	}
	return nil
}

func (p *Node) Getprenode(id *big.Int) Edge {
	Bannode:=make(map[string]bool)
	p.lock.Lock()
	for i:=M-1;i>=0;i-- {
		if _,ok:=Bannode[p.finger[i].Adr];ok {
			p.finger[i]=Edge{p.Adr,p.id}
		} else if Incheck(&p.id,id,&p.finger[i].Id) {
			if Ping(p.finger[i].Adr)==nil {
				Pre:=p.finger[i]
				p.lock.Unlock()
				return Pre
			} else {
				Bannode[p.finger[i].Adr]=true
				p.finger[i]=Edge{p.Adr,p.id}
			}
		}
	}
	p.lock.Unlock()
	return Edge{"",*new(big.Int)}
}

func (p *Node) Getsucnode(id *big.Int,ed *Edge) error {
	if p.Fixsuc()!=nil {
		fmt.Println("getsuc1")
		return errors.New("getsuc1")
	}
	p.lock.Lock()
	if Incheck(&p.id,&p.suc[0].Id,id){
		*ed=p.suc[0]
		p.lock.Unlock()
		return nil
	}
	p.lock.Unlock()
	Nxt:=p.Getprenode(id)
	if Nxt.Adr=="" {
		p.lock.Lock()
		Nxt=p.suc[0]
		p.lock.Unlock()
	}
	client:=Dial(Nxt.Adr)
	if client==nil {
		fmt.Println("getsuc2")
		return errors.New("getsuc2")
	}
	err:=client.Call("RPCNode.Getsucnode",id,ed)
	_=client.Close()
	if err!=nil {
		fmt.Println("getsuc3")
		return errors.New("getsuc3")
	}
	return nil
}

func (p *Node) Checkpre() error {
	p.lock.Lock()
	Adr:=p.pre.Adr
	p.lock.Unlock()
	if Adr!=""&&Ping(Adr)!=nil {
		p.dt.lock.Lock()
		p.predt.lock.Lock()
		for key,val:=range p.predt.mp {
			p.dt.mp[key]=val
		}
		p.dt.lock.Unlock()
		p.predt.lock.Unlock()
		if p.Fixsuc()!=nil{
			fmt.Println("checkpre1")
			return errors.New("checkpre1")
		}
		p.lock.Lock()
		client:=Dial(p.suc[0].Adr)
		p.lock.Unlock()
		if client==nil {
			fmt.Println("checkpre2")
			return errors.New("checkpre2")
		}
		p.dt.lock.Lock()
		mp:=p.dt.mp
		p.dt.lock.Unlock()
		err:=client.Call("RPCNode.Replacepredt",mp,nil)
		_=client.Close()
		if err!=nil {
			fmt.Println("checkpre3")
			return errors.New("checkpre3")
		}
		p.pre.Adr=""
	}
	return nil
}

func (p *Node) Stabilize() error {
	if p.Fixsuc()!=nil {
		fmt.Println("stabilize1")
		return errors.New("stabilize1")
	}
	p.lock.Lock()
	client:=Dial(p.suc[0].Adr)
	p.lock.Unlock()
	if client==nil {
		fmt.Println("stabilize2")
		return errors.New("stabilize2")
	}
	var ed Edge
	err:=client.Call("RPCNode.Getpre",0,&ed)
	_=client.Close()
	if err!=nil {
		fmt.Println(err)
		fmt.Println("stabilize3")
		return errors.New("stabilize3")
	}
	if ed.Adr==p.Adr {
		return nil
	}
	if Ping(ed.Adr)!=nil {
		return nil
	}
	if Incheck(&p.id,&p.suc[0].Id,&ed.Id) {
		err=p.Addlink(ed)
		if err!=nil {
			fmt.Println("stabilize6")
			return errors.New("stabilize6")
		}
	}
	p.lock.Lock()
	client=Dial(p.suc[0].Adr)
	p.lock.Unlock()
	if client==nil {
		fmt.Println("stabilize4")
		return errors.New("stabilize4")
	}
	err=client.Call("RPCNode.Notify",&Edge{p.Adr,p.id},nil)
	_=client.Close()
	if err!=nil {
		fmt.Println(err)
		fmt.Println("stabilize5")
		return errors.New("stabilize5")
	}
	return nil
}

func (p *Node) Maintainsuc() error {
	p.lock.Lock()
	pre:=p.pre.Adr
	p.lock.Unlock()
	if Ping(pre)==nil {
		client:=Dial(pre)
		if client==nil {
			fmt.Println("Maintainsuc1")
			return errors.New("Maintainsuc1")
		}
		var ed [sucl]Edge
		p.lock.Lock()
		for i:=0;i<sucl;i++ {
			ed[i]=p.suc[i]
		}
		p.lock.Unlock()
		err:=client.Call("RPCNode.Updatesuc",&ed,nil)
		_=client.Close()
		if err!=nil {
			fmt.Println("Maintainsuc2")
			return errors.New("Maintainsuc2")
		}
	}
	return nil
}

func (p *Node) Maintain() {
	for p.On {
		if p.innet {
			_=p.Checkpre()
			_=p.Stabilize()
			_=p.Maintainsuc()
		}
		time.Sleep(waitT)
	}
}

func (p *Node) Create() {
	p.lock.Lock()
	p.pre=Edge{p.Adr,p.id}
	p.suc[0]=Edge{p.Adr,p.id}
	for i:=0;i<M;i++ {
		p.finger[i]=Edge{p.Adr,p.id}
	}
	p.lock.Unlock()
	p.innet=true
}

func (p *Node) Join(adr string) error {
	client:=Dial(adr)
	if client==nil {
		fmt.Println("join fail0")
		return errors.New("join fail0")
	}
	var ed Edge
	err:=client.Call("RPCNode.Getsucnode",&p.id,&ed)
	_=client.Close()
	if err!=nil {
		fmt.Println("join fail1")
		return errors.New("join fail1")
	}
	err=p.Addlink(ed)
	p.innet=true
	if err!=nil {
		fmt.Println("join fail -1")
		return errors.New("join fail -1")
	}
	client=Dial(ed.Adr)
	if client==nil {
		fmt.Println("join fail 2")
		return errors.New("join fail 2")
	}
	mp:=make(map[string]string)
	err=client.Call("RPCNode.Movedata",&p.id,&mp)
	p.dt.lock.Lock()
	p.dt.mp=mp
	p.dt.lock.Unlock()
	if err!=nil {
		_=client.Close()
		fmt.Println("join fail 3")
		return errors.New("join fail 3")
	}
	_=client.Close()
	if err!=nil {
		fmt.Println("join fail 6")
		return errors.New("join fail 6")
	}
	for i:=0;i<M;i++ {
		Nxt:=*Addup(&p.id,i)
		var ed Edge
		_=p.Getsucnode(&Nxt,&ed)
		p.lock.Lock()
		p.finger[i]=ed
		p.lock.Unlock()
	}
	return nil
}

func (p *Node) Quit() error {
	if p.Fixsuc()!=nil {
		fmt.Println("quit1")
		return errors.New("quit1")
	}
	if p.suc[0].Adr==p.Adr {
		return nil
	}
	p.lock.Lock()
	client:=Dial(p.suc[0].Adr)
	p.lock.Unlock()
	if client==nil {
		fmt.Println("quit2")
		return errors.New("quit2")
	}
	p.dt.lock.Lock()
	mp:=p.dt.mp
	p.dt.lock.Unlock()
	err:=client.Call("RPCNode.Updatedt",mp,nil)
	if err!=nil {
		_=client.Close()
		fmt.Println("quit3")
		return errors.New("quit3")
	}
	if err!=nil {
		_=client.Close()
		fmt.Println("quit3")
		return errors.New("quit3")
	}
	p.lock.Lock()
	PreAdr:=p.pre.Adr
	p.lock.Unlock()
	if Ping(PreAdr)==nil {
		p.lock.Lock()
		Pre:=p.pre
		p.lock.Unlock()
		err=client.Call("RPCNode.Updatepre",&Pre,nil)
		p.predt.lock.Lock()
		mp=p.predt.mp
		p.predt.lock.Unlock()
		err=client.Call("RPCNode.Replacepredt",mp,nil)
		_=client.Close()
		if err!=nil {
			fmt.Println("quit7")
			return errors.New("quit7")
		}
		client=Dial(PreAdr)
		if client==nil {
			fmt.Println("quit4")
			return errors.New("quit4")
		}
		p.lock.Lock()
		ed:=p.suc[0]
		p.lock.Unlock()
		err=client.Call("RPCNode.Addlink",&ed,nil)
		_=client.Close()
		if err!=nil {
			fmt.Println("quit5")
			return errors.New("quit5")
		}
	} else {
		_=client.Close()
	}
	p.innet=false
	return nil
}

func (p *Node) Queryall(key string,ans *string) error {
	var Nxt Edge
	err:=p.Getsucnode(Hash(key),&Nxt)
	if err!=nil {
		fmt.Println("key not found1")
		return errors.New("key not found1")
	}
	client:=Dial(Nxt.Adr)
	if client==nil {
		fmt.Println("key not found2")
		return errors.New("key not found2")
	}
	err=client.Call("RPCNode.Queryval",key,ans)
	_=client.Close()
	if err!=nil {
		fmt.Println("key not found3")
		return errors.New("key not found3")
	}
	return nil
}

func (p *Node) Erasekey(key string) error {
	p.dt.lock.Lock()
	_,ok:=p.dt.mp[key]
	if !ok {
		p.dt.lock.Unlock()
		return errors.New("erase failed")
	}
	delete(p.dt.mp,key)
	p.dt.lock.Unlock()
	if p.Fixsuc()!=nil {
		fmt.Println("Erase failed in suc")
		return errors.New("Erase failed in suc")
	}
	p.lock.Lock()
	client:=Dial(p.suc[0].Adr)
	p.lock.Unlock()
	if client==nil {
		fmt.Println("Erase failed in suc2")
		return errors.New("Erase failed in suc2")
	}
	err:=client.Call("RPCNode.Erasepreval",key,nil)
	_=client.Close()
	if err!=nil {
		fmt.Println("Erase failed in suc3")
		return errors.New("Erase failed in suc3")
	}
	return nil
}

func (p *Node) Eraseall(key string) error {
	var Nxt Edge
	err:=p.Getsucnode(Hash(key),&Nxt)
	if err!=nil {
		fmt.Println("erase key not found")
		return errors.New("erase key not found1")
	}
	client:=Dial(Nxt.Adr)
	if client==nil {
		fmt.Println("erase key not found2")
		return errors.New("erase key not found2")
	}
	err=client.Call("RPCNode.Erasekey",key,nil)
	_=client.Close()
	if err!=nil {
		fmt.Println("erase key not found3")
		return errors.New("erase key not found3")
	}
	return nil
}

func (p *Node) Insertself(ky Keyval) error {
	p.dt.lock.Lock()
	p.dt.mp[ky.Key]=ky.Val
	p.dt.lock.Unlock()
	if p.Fixsuc()!=nil {
		fmt.Print("insertself1")
		return errors.New("insertself1")
	}
	p.lock.Lock()
	client:=Dial(p.suc[0].Adr)
	p.lock.Unlock()
	if client==nil {
		fmt.Print("insertself2")
		return errors.New("insertself2")
	}
	err:=client.Call("RPCNode.Insertpre",ky,nil)
	_=client.Close()
	if err!=nil {
		fmt.Print("insertself3")
		return errors.New("insertself3")
	}
	return nil
}

func (p *Node) Insert(key string,val string) error {
	var ed Edge
	err:=p.Getsucnode(Hash(key),&ed)
	if err!=nil {
		fmt.Println("insert 1")
		return errors.New("insert 1")
	}
	client:=Dial(ed.Adr)
	if client==nil {
		fmt.Println("insert 2")
		return errors.New("insert 2")
	}
	err=client.Call("RPCNode.Insertself",Keyval{key,val},nil)
	_=client.Close()
	if err!=nil {
		fmt.Println("insert 3")
		return errors.New("insert 3")
	}
	return nil
}
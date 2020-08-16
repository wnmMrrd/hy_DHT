package chord

import (
	"errors"
	"math/big"
	"net"
)

type RPCNode struct {
	Nd *Node
	Listen net.Listener
}

func (p *RPCNode) Getdt(_ int,mp *map[string]string) error {
	p.Nd.dt.lock.Lock()
	*mp=p.Nd.dt.mp
	p.Nd.dt.lock.Unlock()
	return nil
}

func (p *RPCNode) Movedata(Pos *big.Int,mp *map[string]string) error {
	p.Nd.dt.lock.Lock()
	for key,val:=range p.Nd.dt.mp {
		if !Incheck(Pos,&p.Nd.id,Hash(key)) {
			(*mp)[key]=val
		}
	}
	p.Nd.dt.lock.Unlock()
	p.Nd.predt.lock.Lock()
	p.Nd.predt.mp=*mp
	p.Nd.predt.lock.Unlock()
	return nil
}

func (p *RPCNode) Updatedt(mp map[string]string,_ *int) error {
	p.Nd.dt.lock.Lock()
	for key,val:=range mp {
		p.Nd.dt.mp[key]=val
	}
	p.Nd.dt.lock.Unlock()
	return nil
}

func (p *RPCNode) Updatesuc(ed *[sucl]Edge,_ *int) error {
	p.Nd.lock.Lock()
	for i:=1;i<sucl;i++ {
		p.Nd.suc[i]=ed[i-1]
	}
	p.Nd.lock.Unlock()
	return nil
}

func (p *RPCNode) Replacepredt(mp map[string]string,_ *int) error {
	p.Nd.Replacepredt(mp)
	return nil
}

func (p *RPCNode) Updatepre(ed *Edge,_ *int) error {
	p.Nd.lock.Lock()
	p.Nd.pre=*ed
	p.Nd.lock.Unlock()
	return nil
}

func (p *RPCNode) Notify(Pre *Edge,_ *int) error {
	return p.Nd.Notify(*Pre)
}

func (p *RPCNode) Getsucnode(id *big.Int,ed *Edge) error {
	return p.Nd.Getsucnode(id,ed)
}

func (p *RPCNode) Getpre(_ *int,ed *Edge) error {
	p.Nd.lock.Lock()
	*ed=p.Nd.pre
	p.Nd.lock.Unlock()
	return nil
}

func (p *RPCNode) Addlink(Nxt *Edge,_ *int) error {
	return p.Nd.Addlink(*Nxt)
}

func (p *RPCNode) Queryval(key string,ans *string) error {
	p.Nd.dt.lock.Lock()
	val,ok:=p.Nd.dt.mp[key]
	p.Nd.dt.lock.Unlock()
	if ok {
		*ans=val
		return nil
	} else {
		return errors.New("value not found")
	}
}

func (p *RPCNode) Erasepreval(key string,_ *int) error {
	p.Nd.predt.lock.Lock()
	_,ok:=p.Nd.predt.mp[key]
	if ok {
		delete(p.Nd.predt.mp,key)
	}
	p.Nd.predt.lock.Unlock()
	return nil
}

func (p *RPCNode) Erasekey(key string,_ *int) error {
	return p.Nd.Erasekey(key)
}

func (p *RPCNode) Insertpre(ky Keyval,_ *int) error {
	p.Nd.predt.lock.Lock()
	p.Nd.predt.mp[ky.Key]=ky.Val
	p.Nd.predt.lock.Unlock()
	return nil
}

func (p *RPCNode) Insertself(ky Keyval,_ *int) error {
	return p.Nd.Insertself(ky)
}

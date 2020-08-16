package chord

import (
	"crypto/sha1"
	"errors"
	"math/big"
	"net/rpc"
	"time"
)

var(
	Two=big.NewInt(2)
	mod=new(big.Int).Exp(Two,big.NewInt(160),nil)
)

func Hash(str string) *big.Int {
	val:=sha1.New()
	val.Write([]byte(str))
	return new(big.Int).SetBytes(val.Sum(nil))
}

func Addup(val *big.Int,pow int) *big.Int {
	pval:=new(big.Int).Exp(Two,big.NewInt(int64(pow)),nil)
	sum:=new(big.Int).Add(val,pval)
	return new(big.Int).Mod(sum,mod)
}

func Incheck(l,r,x *big.Int) bool {
	r2:=new(big.Int)
	if l.Cmp(r)>=0 {
		r2.Add(r,mod)
	} else{
		r2=r
	}
	if l.Cmp(x)<0&&x.Cmp(r2)<=0 {
		return true
	}
	x2 := new(big.Int).Add(x, mod)
	if l.Cmp(x2)<0&&x2.Cmp(r2)<=0 {
		return true
	}
	return false
}

func Dial(adr string) *rpc.Client {
	for i:=1;i<=5;i++ {
		client,err:=rpc.Dial("tcp",adr)
		if err!=nil{
			time.Sleep(time.Millisecond*50)
		} else {
			return client
		}
	}
	return nil
}

func Ping(adr string) error {
	for i:=1;i<=5;i++ {
		client,err:=rpc.Dial("tcp",adr)
		if(err!=nil){
			time.Sleep(time.Millisecond*50)
		} else {
			_=client.Close()
			return nil
		}
	}
	return errors.New("Ping Fail")
}
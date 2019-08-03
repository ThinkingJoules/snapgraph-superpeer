let lmdb = require('node-lmdb');
let snapUtil = require('snapgraph/core/util')

const NULL = String.fromCharCode(0)
const IS_STRINGIFY = String.fromCharCode(1)

const GUN_NODE = String.fromCharCode(17)
const IS_DATA = String.fromCharCode(18)
const IDX = String.fromCharCode(19)




const RS = String.fromCharCode(30)
const ESC = String.fromCharCode(27)



const enq = String.fromCharCode(5)
const tru = String.fromCharCode(6)
const fal = String.fromCharCode(21)

const gl = String.fromCharCode(26)
const prim = String.fromCharCode(2)
const gs = String.fromCharCode(29)
const us = String.fromCharCode(31)

const ENCODING = 'utf16le'

function LMDB(config){
    this.env = new lmdb.Env()
    const self = this

    self.env.open(config)
    this.dbi = self.env.openDbi({
        name: 'gun-nodes',
        create: true,
        keyIsBuffer:true
    })
    this.putData = function(root,soul,putO,expire,msgIDs,cb){
        //puts = {soul:{[msgIDs]:[],putO:{gunPutObj(partial)}}
        let txn = self.env.beginTxn()
        try {
            putDataSoul(soul,putO,expire)
            txn.commit()
            cb(false,true)
            sendAcks(false)
            return
        } catch (error) {
            console.log("ERROR IN TXN",error)
            txn.abort()
            cb(error,false)
            sendAcks(error)
        }
        function sendAcks(error){
            for (const msgID of msgIDs) {
                root.on('in', {
                    '@': msgID,
                    ok: !error,
                    err: (error === false) ? error : error.toString()
                });
            }
            
        }
        function putDataSoul(soul,put,exp){
            let soulKey = makeKey(soul)
            let rawPs = txn.getBinary(self.dbi,soulKey)
            let pvals = rawPs && JSON.parse(rawPs.toString(ENCODING)) || []
            let now = Date.now()
            for (const p in put) {
                if (p === '_')continue
                let val = put[p]
                let expire = exp[p]
                let addrKey = makeKey(soul,p)
                if(expire && now>expire){
                    let exists = txn.getBinary(self.dbi,addrKey)
                    if(exists !== null)txn.del(self.dbi,addrKey)
                    continue
                }
                if(val !== null){
                    if(!pvals.includes(p))pvals.push(p)
                    let str
                    let ham = snapUtil.getValue(['_','>',p],put) || now
                    if(typeof val === 'string')str = NULL+val
                    else str = IS_STRINGIFY+JSON.stringify(val)
                    str += ESC + ham
                    if(expire)str+=RS+String(expire)
                    //str is STRINGFY_FLAG + [string buffer] + ESC + [ham string buffer] [optional: + RS + expire string buffer]
                    let encodedVal = Buffer.from(str,ENCODING)
                    txn.putBinary(self.dbi,makeKey(soul,p),encodedVal,{keyIsBuffer:true})
                }else{
                    if(pvals.includes(p)){
                        snapUtil.removeFromArr(pvals,pvals.indexOf(p))
                        txn.del(self.dbi,makeKey(soul,p),{keyIsBuffer:true})
                    }
                }
            }
            pvals.sort()//make lexical??
            txn.putBinary(self.dbi,soulKey,Buffer.from(JSON.stringify(pvals),ENCODING),{keyIsBuffer:true})
            txn.putBinary(self.dbi,makeKey(soul,'length'),Buffer.from(String(pvals.length),ENCODING),{keyIsBuffer:true})//# of non-null-value keys
        }
    }
    this.getBatch = function(root,batch,cb){//untested, doesn't work
        let s = Date.now()
        let out = {} //{soul:{validPutObj}}
        let txn = self.env.beginTxn({readOnly:true})
        let fromMem = 0,fromDisk = 0
        try {
            for (const soul in batch) {
                const arrOfProps = batch[soul];
                out[soul] = {'_':{'#':soul,'>':{}}}
                for (const prop of arrOfProps) {
                    let inMem = snapUtil.getValue(['graph',soul,prop],root)
                    if(inMem !== undefined){
                        //console.log('IN mem')
                        fromMem++
                        out[soul][prop] = inMem
                        out[soul]['_']['>'][prop] = snapUtil.getValue(['graph',soul,'_','>',prop],root)
                    }else{
                        //console.log('DISK')
                        fromDisk++
                        let [val,ham] = self.getProp(soul,prop,out,txn) || []
                        if(val){//need to set enough vals so it is a valid thing in the graph
                            snapUtil.setValue(['graph',soul,'_','#'],soul,root)
                            snapUtil.setValue(['graph',soul,prop],val,root)
                            snapUtil.setValue(['graph',soul,'_','>',prop],ham,root)
                        }
                    }
                }
            }
            console.log('RETRIEVED GETBATCH IN',(Date.now()-s)+'ms',{fromMem,fromDisk})
            cb(false,out)
            txn.commit()
            return out
        } catch (error) {
            cb(error)
            txn.commit()
            return error
        }
    }
    this.get = function(soul,prop){
        //console.log(soul,snapUtil.DATA_INSTANCE_NODE.test(soul))
        let txn = self.env.beginTxn({ readOnly: true })
        let put = {[soul]:{'_':{'#':soul,'>':{}}}}
        let hasOne = true
        if(prop && !Array.isArray(prop))self.getProp(soul,prop,put,txn)
        else{
            let soulKey = makeKey(soul)
            let raw = txn.getBinary(self.dbi,soulKey,{keyIsBuffer:true})
            if(raw === null) {
                txn.commit()
                return null
            }
            if(raw[0] === Buffer.from(GUN_NODE,ENCODING)[0]){//is a full valid gun node (not stored by props)
                put = JSON.parse(raw.toString(ENCODING,1))//reassign put
            }else{//decomposed gun node
                hasOne = false
                let props
                if(prop && Array.isArray(prop)){//this isn't ever used. only valid gun calls come through here.. this is in snapGet/getBatch api
                    props = prop
                }else{
                    props = JSON.parse(raw.toString(ENCODING))
                }
                //console.log('GETTING',soul,props)
                for (const prop of props) {
                    let got = self.getProp(soul,prop,put,txn)
                    if(!hasOne && !!got)hasOne = true
                }
            } 
        }
        txn.commit()
        //console.log('get REPLY (put)',put)
        return (hasOne) ? put : null
        
    }
    this.getProp = function (soul,prop,msgPut,txn){
        let addr = makeKey(soul,prop)
        let value = txn.getBinary(self.dbi,addr,{keyIsBuffer:true})
        let meta = msgPut[soul]['_']['>']
        let val,ham,exp
        let now = Date.now()
        if(value === null){
            val = null
            ham = now //should alway overwrite old graph data if the get got to the disk?
            exp = Infinity
        }else{
            let eSplit = value.indexOf(Buffer.from(ESC,ENCODING),0,ENCODING)
            let exSplit = value.indexOf(Buffer.from(RS,ENCODING),0,ENCODING)
            exSplit = (exSplit === -1) ? 0 : exSplit
            ham = value.toString(ENCODING,eSplit+2,(exSplit||value.length))*1//get HAM to number
            exp = (exSplit) ? value.toString(ENCODING,exSplit+2)*1 : Infinity
            if(value[0] === Buffer.from(IS_STRINGIFY,ENCODING)[0]){
                val = JSON.parse(value.toString(ENCODING,2,eSplit))
            } else {
                val = value.toString(ENCODING,2,eSplit)
            }
        }
        if(now<exp){
            msgPut[soul][prop] = val
            meta[prop] = ham
            return [val,ham]//let it know that it found something so we don't return an empty node
        }
        console.log("REMOVING EXPIRED VALUE")
        txn.del(self.dbi,addr)
    }
    this.getLength = function(soul){
        let txn = self.env.beginTxn({ readOnly: true })
        let val = txn.getBinary(self.dbi,makeKey(soul,'length'),{keyIsBuffer:true})
        val = val && val.toString(ENCODING)*1 || 0
        txn.commit()
        return val
    }
}
function makeKey(soul,prop){
    //console.log('MAKING',soul,prop)
    soul = String(soul)
    if(prop!==undefined){
        prop = String(prop)
        return Buffer.from(soul+RS+prop,ENCODING)
    }
    return Buffer.from(soul,ENCODING)
}
module.exports = {LMDB}
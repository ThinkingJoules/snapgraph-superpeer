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
    this.putData = function(root,putBuffer){
        //puts = {soul:{[msgIDs]:[],putO:{gunPutObj(partial)}}
        let txn = self.env.beginTxn({readOnly:true})
        let results = {}
        let toWrite = []
        let i = 0
        for (const [soul,{msgIDs,putO}] of putBuffer.entries()) {
            results[soul] = {range:[i],msgIDs}
            try {
                if(snapUtil.ALL_ADDRESSES.test(soul)){// || IS_INDEX_SOUL
                    console.log('DATA SET NODE')
                    putDataSet(soul,putO)
                }else{
                    putDataSoul(soul,putO)
                }
                results[soul].range.push(i)//i is incremented, [i,j] is segment of batchwrite results that indicate if soul succeeded
            } catch (error) {//if the error comes from txn does it break everything?
                console.log("ERROR IN PUT DECOMPOSITION:",{soul,error})
            }
            putBuffer.delete(soul)
        }
        txn.commit()
        self.env.batchWrite(toWrite,{keyIsBuffer:true},function(error,resArray){
            if(resArray){
                for (const soul in results) {
                    const {range,msgIDs} = results[soul];
                    let fail = /[^0]/.test(resArray.slice(...range).join(''))
                    for (const msgID of msgIDs) {
                        root.on('in', {
                            '@': msgID,
                            ok: !fail,
                            err: fail
                        });
                    }
                }
            }
            if(error){console.log("ERROR IN BATCH PUT:",error)}
        })
        function putDataSoul(soul,put){
            let soulKey = makeKey(soul)
            let pvals = txn.getBinary(self.dbi,soulKey) || Buffer.alloc(0)//p collection so we can delete all things if needed
            let newP = ''
            let now = Date.now()
            for (const p in put) {
                if (p === '_')continue
                if(!pvals.includes(Buffer.from(p,ENCODING)))newP+=p+RS
                let val = put[p],str = IS_DATA
                let ham = snapUtil.getValue(['_','>',p],put) || now
                //if(typeof val === 'object' && val !== null && !Array.isArray(val))continue//regular souls will stringify links '{'#':'someSoul'}
                if(typeof val === 'string')str+= NULL+val
                else str += IS_STRINGIFY+JSON.stringify(val)
                str += ESC + ham
                //str is ISDATA + STRINGFY_FLAG + [string buffer] + ESC + [ham string buffer]
                let encodedVal = Buffer.from(str,ENCODING)
                //txn.putBinary(self.dbi,makeKey(soul,p),encodedVal,{keyIsBuffer:true})
                toWrite.push([self.dbi,makeKey(soul,p),encodedVal])
                i++
            }
            let pBuffer = Buffer.from(pvals.toString(ENCODING)+newP,ENCODING)//newP+'\0' 
            if(newP.length){
                //txn.putBinary(self.dbi,soulKey,pBuffer,{keyIsBuffer:true})
                toWrite.push([self.dbi,soulKey,pBuffer]) 
                i++
            }
        }
        function putDataSet(soul,put){//only specific souls are stored like this
            let dbKey = makeKey(soul)
            let existing = txn.getBinary(self.dbi,dbKey,{keyIsBuffer:true})
            if(existing === null)existing = Buffer.from("{}",ENCODING)
            let obj = JSON.parse(existing.toString(ENCODING))
            for (const thing in put) {
                if (thing === '_')continue
                const boolean = put[thing];
                if(boolean !== null){//add to set
                    obj[key] = boolean
                    snapUtil.setValue(['_','>',key],snapUtil.getValue(['_','>',key],put)||Date.now(),obj)//set HAM value OR make it NOW
                }else{//remove from set,this may get re-added if other things persist it, all snapsuperPeers will attempt to prune list.
                    delete obj[key]
                    if(obj['_'] && obj['_']['>'] && obj['_']['>'][key])delete obj['_']['>'][key]
                }
            }
            //txn.putBinary(self.dbi,dbKey,Buffer.from(GUN_NODE+JSON.stringify(obj),ENCODING),{keyIsBuffer:true})
            toWrite.push([self.dbi,dbKey,Buffer.from(GUN_NODE+JSON.stringify(obj),ENCODING)])
            i++
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
                let props = []
                if(prop && Array.isArray(prop)){
                    props = prop
                }else{
                    let raw = txn.getBinary(self.dbi,soulKey,{keyIsBuffer:true})
                    let i = 0
                    while(true){
                        let j = raw.indexOf(RS,i+1,ENCODING)
                        if(j-i < 0)break
                        props.push(raw.toString(ENCODING,i,j))
                        i = j+2//1 for uft8, 2 for utf16le?
                    }
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
    this.getProp = function (soul,prop,putObj,txn){
        let addr = makeKey(soul,prop)
        let value = txn.getBinary(self.dbi,addr,{keyIsBuffer:true})
        if(value === null)return//prop is non-existent, so don't put anything in the obj
        let meta = putObj[soul]['_']['>']
        let val,ham
        if(value[0] === Buffer.from(IS_DATA,ENCODING)[0]){
            let eSplit = value.indexOf(Buffer.from(ESC,ENCODING),0,ENCODING)
            meta[prop] = ham = value.toString(ENCODING,eSplit+2)*1//get HAM to number
            if(value[2] === Buffer.from(IS_STRINGIFY,ENCODING)[0]){
                val = JSON.parse(value.toString(ENCODING,4,eSplit))
            } else {
                val = value.toString(ENCODING,4,eSplit)
            }
            putObj[soul][prop] = val
        }else{//is full node (like a set?) 
            console.log('THIS SHOULD NEVER RUN I THINK IT CAN BE REMOVED')
            putObj[soul] = JSON.parse(value.toString(ENCODING,2))
        }
        return [val,ham]//let it know that it found something so we don't return an empty node
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
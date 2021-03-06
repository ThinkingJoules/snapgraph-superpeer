let snapUtil = require('snapgraph/core/util')
const toGun = (Gun,lmdb) =>{

    const gets = new Map()
    const puts = new Map()
    let startPut
    Gun.on('create', function (root) {
        root.on('out',function(msg,peer){

            //console.log('OUT----',root.opt.peers)
            this.to.next(msg);
            // if(msg && msg['@'] && msg.how && msg.how == 'mem'){
            //     console.log(msg)
            //     console.log('REMOVING FROM BUFFER',msg['@'],'gets',gets)
            //     gets.delete(msg['@'])
            // }
            //console.log('OUT',msg)
        })
        root.on('in',function(msg){
            console.log('IN----',msg)
            if(msg && msg.snapGet){//this is to limit number of wire calls
                //console.log('NEW getBatch, processing...')
                let req = msg.snapGet
                lmdb.getBatch(root,req,function(er,res){
                    if(er){
                        console.log("ERROR IN getBatch",er)
                        return
                    }
                    root.on('in',{
                        '@':msg['#'],
                        subGraph: res
                    })
                })
                
                return
            }
            if(msg && msg.getLength){
                root.on('in',{
                    '@':msg['#'],
                    length: lmdb.getLength(msg.getLength)
                })
                return
            }
            this.to.next(msg);
            if(msg && msg['@'] && msg.how && msg.how == 'mem' && !msg['##']){
                //console.log('REMOVING FROM BUFFER',msg['@'],'gets',gets)
                gets.delete(msg['@'])
            }
            
            //console.log('IN',msg)
        })
        root.on('get', function (msg) {
            //console.log('GET',msg)
            this.to.next(msg);
            if (!msg)return;//ignore get hash messages for disk (This is from an 'out' on our Gun instance)?????????
            const msgID = msg['#'];
            const get = msg.get;
            const soul = get['#'];
            const prop = get['.'];
            //console.log('root graph for ',soul, root.graph[soul])

            gets.set(msgID,[soul,prop])
            //console.log('ADDED GET TO BUFFER',msg)
            if(!gets.pending){
                gets.pending = true
                setTimeout(getBuffer,5)
            }
            
        });
        root.on('put', function (msg) {
            //console.log('PUT channel',msg.how)
            if (!msg){this.to.next(msg);return}
            if (msg.how && msg.how === 'disk'){this.to.next(msg);return}//?
            console.log('PUT>TO DISK',msg)
            const msgID = msg['#'];
            let soul = Object.keys(msg.put)[0]
            let put = msg.put[soul]
            let {msgIDs,putO,expire} = puts.get(soul) || {}
            if(!putO)puts.set(soul,{msgIDs:[msgID],putO:put,expire:(msg.expire || {})})
            else {
                msgIDs.push(msgID)
                snapUtil.mergeObj(putO,put)//should mutate the object in the map, no need for setting it.
                snapUtil.mergeObj(expire,(msg.expire||{}))
            }
            if(!puts.pending){
                puts.pending = true
                setTimeout(putBuffer,50)//not sure on value, want it pretty large to help get importing/blasts eff, but don't want to delay other writes?
                //could be pretty long I suppose, in-mem graph should respond, and the buffer is merging updates already...
                //if you made this really(ish) long could make a sort of superPeer sync block..
                //this is getting in to blockchain-y things I think...
                //would need to think of how best to keep superPeers in sync without excessive traffic over wire/sending updates to clients..
            }
            
            this.to.next(msg);
        });
        this.to.next(root);
        function getBuffer(){
            if(gets.getting){//wait for first calls to finish
                setTimeout(getBuffer,1)
            }
            //console.log('GETTING BUFFER....')
            if(!gets.size){
                //console.log('nothing in get buffer')
                gets.pending = false
                return
            }
            gets.getting = true
            //console.log('FROM DISK:',gets)
            console.log('GETTING',gets.size,'THINGS FROM DISK')
            for (const [msgID,[soul,prop]] of gets.entries()) {
                let value = lmdb.get(soul,prop)//could batch all these in one txn? Wouldn't be gun compatible. one ack per msg
                let error = (value instanceof Error) ? value : false
                if (error) {
                    console.error('error', error);
                    root.on('in', {
                        '@': msgID,
                        put: null,
                        err: error,
                        how: 'disk'
                    });
                }
                else {
                    root.on('in', {
                        '@': msgID,
                        put: value || null,
                        err: null,
                        how: 'disk'
                    });
                }
                gets.delete(msgID)
            }
            gets.getting = false
            getBuffer()
            
        }
        function putBuffer(){
            if(puts.putting){//wait for first calls to finish
                setTimeout(putBuffer,1)
            }
            if(!puts.size){
                puts.pending = false
                console.log('COMMITED PUT BUFFER IN:',(Date.now()-startPut)+'ms')
                startPut = undefined
                return
            }
            if(startPut === undefined)startPut = Date.now()
            puts.putting = true
            let [[soul,{msgIDs,putO,expire}]] = puts.entries()
            lmdb.putData(root,soul,putO,expire,msgIDs,function(error,results){
                puts.delete(soul)
                puts.putting = false
                setTimeout(putBuffer,1)
            })            
        }
    });
    
}
module.exports = {toGun}
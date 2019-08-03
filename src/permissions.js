//PERMISSIONS (from first tests... all is broken.)
let authdConns = {}
function clientAuth(ctx){
    let root = ctx.root
    let msg = {}
    msg.creds = Object.assign({},root.user.is)
    msg['#'] = Gun.text.random(9)
    Gun.SEA.sign(msg['#'],root.opt.creds,function(sig){
        Gun.SEA.encrypt(sig,root.opt.pid,function(data){
            msg.authConn = data
            root.on('out',msg)
        })
    })
}
function verifyClientConn(ctx,msg){
    let root = ctx.root
    let ack = {'@':msg['#']}
    let{authConn,creds} = msg
    let pid = msg._ && msg._.via && msg._.via.id || false
    if(!pid){console.log('No PID'); return;}
    Gun.SEA.decrypt(authConn,pid,function(data){
        if(data){
            Gun.SEA.verify(data,creds.pub,function(sig){
                if(sig !== undefined && sig === msg['#']){
                    //success
                    authdConns[pid] = creds.pub
                    console.log("AUTH'd Connections: ",authdConns)
                    root.on('in',ack)
                }else{
                    ack.err = 'Could not verify signature'
                    root.on('in', ack)
                    //failure
                }
            })
        }else{
            console.log('decrypting failed')
        }
    })
}
function clientLeft(msg){
    let pid = msg && msg.id || false
    if(pid){
        delete authdConns[pid]
        console.log('Removed: ',pid, ' from: ',authdConns)
    }
}
function addHeader(ctx,msg,to){//no longer needed?
    let pair = ctx.opt.creds
    let type = (msg.get) ? 'get' : (msg.put) ? 'put' : false
    msg.header = {type,pub:false,sig:false}
    if(pair && type){
        let pub = pair.pub
        msg.header.pub = pub
        msg.header.token = token
        msg.header.sig = tokenSig
        
        to.next(msg)
        // let toSign = msg['#'] || msg['@'] //msg ID as entropy
        // Gun.SEA.sign(toSign,pair,function(sig){
        //     if(sig !== undefined){
        //         msg.header = {pub:pub,sig,alias:pair.alias}
        //         //console.log('HEADER ADDED: ',msg)
        //         to.next(msg)
        //     }else{
        //         to.next(msg)
        //     }
        // })
    }else{
        to.next(msg)
    }
    //console.log('OUT: ',msg)
}

function verifyPermissions(ctx,msg,to){
    if(msg.get && msg.get['#']){// get
        verifyOp(ctx,msg,to,'get')
    }else if (msg.put && Object.keys(msg.put).length){// put
        verifyOp(ctx,msg,to,'put')
    }else{
        to.next(msg)
    }
}

function isRestricted(soul,op){
    let getWhiteList = [/~/,/\|/,/GBase/,/config/,/\/t$/,/\/t\d*\/p/]
    if(op === 'get'){
        for (const t of getWhiteList) {
            let p = t.test(soul)
            if(p){
                return false
            }
        }
        //console.log('not on whiteList:', soul)
        let isGBase = /\/t\d+/g.test(soul) //looks for anything that has = '/t' + Number() (that didn't pass the whiteList)
        if(isGBase)return true
        return false //default everything else to read w/o login
    }else{
        if(/~/.test(soul))return false //allow user puts
        if(/GBase/.test(soul))return false //allow additions to list of bases
        
        return true //default all other puts to needing permission
    }
}
// let validTokens = {}
// const expireTok = (tok) =>{
//     delete validTokens[tok]
// }
function verifyOp(ctx,msg,to,op){
    let root = ctx.root
    let pobj = {msg,to,op}
    pobj.pub = false
    pobj.verified = false
    pobj.soul = (op==='put') ? Object.keys(msg.put)[0] : msg.get['#']
    pobj.prop = (op==='put') ? msg.put[pobj.soul] : msg.get['.']
    pobj.who = msg._ && msg._.via && msg._.via.id || false
    if(!isRestricted(pobj.soul,pobj.op)){//no auth needed
        //console.log('No auth needed: ',pobj.soul)
        to.next(msg)
        return
    }
    let authdPub = authdConns[pobj.who]
    //console.log('MSG FROM: ',pobj.who,' PUB: ',authdPub)
    if(pobj.who && authdPub){
        //console.log('Valid Connection')
        pobj.verified = true
        pobj.pub = authdConns[pobj.who]
        testRequest(root,pobj)
    }
    // if(msg.header && msg.header.sig && msg.header.pub && msg.header.token){
    //     if(msg.header.token !== 0 && validTokens[msg.header.token] && validTokens[msg.header.token] === msg.header.pub){
    //         console.log('Valid Token!')
    //         pobj.verified = true
    //         pobj.pub = msg.header.pub
    //         testRequest(root,pobj,pobj.soul)
    //     }else{
    //         let {pub,sig,token} = msg.header
    //         Gun.SEA.verify(sig,pub,function(data){
    //             if(data !== undefined && data === token){
    //                 console.log('Valid Sig!')
    //                 pobj.verified = true
    //                 pobj.pub = pub 
    //                 validTokens[token] = pub
    //                 setTimeout(expireTok,20000,token)
    //             }
    //             if(pobj.verified){
    //                 console.log('Message Sender Verified ', pobj.soul)
    //             }else{
    //                 //console.log('NOT VERIFIED: Sig/Pub mismatch',msg)
    //             }
    //             testRequest(root,pobj,pobj.soul)
    //         })
    //     }
    //     console.log('Checking ', pobj.op,': ', pobj.soul)
        
    // }
    else{//not logged in, could potentially have permissions?
        console.log('No/Empty message header! Attempting access to soul: ',pobj.soul)
        testRequest(root,pobj)
    }
}
let permCache = {}
function testRequest(root, request, testSoul){
    let {pub,msg,to,verified,soul,prop,op} = request
    testSoul = testSoul || soul
    if(!gb)throw new Error('Cannot find GBase config file') //change to fail silent for production
    let [path,...perm] = soul.split('|')
    let [base,tval,...rest] = testSoul.split('|')[0].split('/') //path === testSoul if not a nested property
    if(soul.includes('timeLog') || soul.includes('timeIndex')){
        path = path.split('>')[1]
        let[b,t,...i] = testSoul.split(':')[0].split('>')[1].split('/')
        base = b
        tval = t
        rest = (i) ? i[0].split('/') : i
    }
    let own = getValue([base,'props',tval,'owner'],gb) || false //false === row perms will be overridden by table perms
    let inherit = getValue([base,'inherit_permissions'],gb) || true // true === row will inherit table perms which will inherit base perms if missing
    let traverse = true
    let reqType
    
    if(soul.includes('|')){//permission change (put),(get is whitelisted)
        //console.log('Permission msg: ',msg)
        if((soul.includes('|super') || soul.includes('|group/admin|permissions')) && pub && verified){//attempt to modify 'baseID|super' node
            console.log('Attempting to create a Super Admin or Admin group')
            getSoul(soul,pub,true,function(data){
                //console.log(soul, ' IS: ',data)
                if(data && soul.includes('|super')){
                    console.log('Already exists! ',data)
                    root.on('in',{'@': msg['#'],  err: 'There is already a Super Admin for this base'})
                }else if(!data && soul.includes('|super')) {
                    //console.log('Creating new super node for new base')
                    to.next(msg)
                }else if(!data){
                    isSuper()
                }else{
                    attemptCHP(data,'group')
                }
            })
        }else if(soul.includes('|group/')){//group or group permission options
            if(soul.includes('permissions')){
                // console.log('CREATING GROUP PERMISSIONS')
                getSoul(soul,false, true ,function(data){
                    if(data){
                        attemptCHP(data,'group')
                    }else{//if node doesn't exist
                        isGrpOwner()//must have rowID|permission node created before creating group/...|permissions
                    }
                })
            }else{//changing membership
                //console.log('CHANGING MEMBERSHIP')
                let perms = soul + '|permissions'
                getSoul(perms,false,true,function(val){
                    //console.log('GROUP CHANGE, pubVerified: ', verified, 'pub: ', pub)
                    if(val){
                        addRemoveMember(val)
                    }else{// no permissions node for group. Must be admin?
                        isGrpOwner()
                    }
                })
            }
            
        }else if(soul.includes('permissions')){//for permission nodes themselves, sould be either base, table, or row permissions
            if(rest[0] === undefined){// baseID|permissions || baseID/tval|permissions : must be admin or super
                isAdmin()
            }else{//row permission
                //soul = baseID/tval/rval|permissions
                getSoul(soul,false, true, function(data){
                    if(data){
                        attemptCHP(data,'row') //editing existing soul
                    }else{//if node doesn't exist
                        //find 'create' permissions
                        checkScope('create') //creating this node
                    }
                })
            }
            
        }else if(soul.includes('|groups')){
            isAdmin()
        }
    }else if(soul.includes('config') && pub && verified){//no permissions node on config, must be admin or super
        isAdmin()
    }else{//all other restricted nodes, should be rows, can be 'get' or 'put'
        if(rest && rest[0] && rest[0][0] && rest[0][0] === 'r' || rest[0] === 'created'){// is some sort of row..
            let path
            if(rest[0] === 'created'){
                path = [base,tval].join('/')
            }else{
                path = [base,tval,rest[0]].join('/')
                reqType = 'row'
            }
            let permSoul = path +'|permissions'
            let hasNext = hasPropType(gb,[base,tval].join('/'),'next') //false || [pval]
            let opAs = (soul !== testSoul) ? 'get' : op
            if(!hasNext)traverse = false
            if(inherit || !own){
                checkScope(isOp(false, opAs))///read || create
            }else{
                getSoul(permSoul,false,true,function(val){
                    if(val){
                        testPermissions(val,isOp(true, opAs))
                    }else{
                        isAdmin('ERROR: NO PERMISSIONS FOUND!') 
                    }
                })
            }
            
        }else if(rest && rest[0] && rest[0][0] && rest[0][0] === 'p'){
            //going to deprecate these nodes in gbase soon.
            to.next(msg)
            //column soul base/tval/pval
        }else if(!rest){
            //base or base/tval
            to.next(msg)
        }else{
            //doesn't match anything in gbase
            isAdmin('Invalid Soul')
        }
        function isOp(exists, opAs){
            let tryOp = opAs || op
            if(tryOp === 'get'){
                return 'read'
            }
            if(exists){
                return 'update'
            }else{
                return 'create'
            }
        }
    }
    function checkScope(operation){
        let bPerm = base+'|permissions'
        let tPerm = [base,tval].join('/') +'|permissions'
        getSoul(tPerm,false,true,function(val){
            if(val){
                testPermissions(val,operation)
            }else if(inherit){
                getSoul(bPerm,false,true,function(val){
                    if(val){
                        testPermissions(val,operation)
                    }else{
                        isAdmin('ERROR: NO PERMISSIONS TO INHERIT!!')
                    }
                })
            }else{
                isAdmin('ERROR: NO PERMISSIONS TO INHERIT!!')  
            }
        })
    }
    function lookLocal(soul,prop,cb) {
        //console.log('lookLocal, ',soul,prop)
        if(!isNode){
            return undefined
        }
        prop = prop || ''
        cb = (cb instanceof Function && cb) || console.log
        var id = msg['#'], has = prop, opt = {}, graph, lex, key, tmp;
        if(typeof soul == 'string'){
            key = soul;
        } 
        //else 
        // if(soul){
        //     if(tmp = soul['*']){ opt.limit = 1 }
        //     key = tmp || soul['='];
        // }
        if(key && !opt.limit){ // a soul.has must be on a soul, and not during soul*
            if(typeof has == 'string'){
                key = key+esc+(opt.atom = has);
            }
            // else 
            // if(has){
            //     if(tmp = has['*']){ opt.limit = 1 }
            //     if(key){ key = key+esc + (tmp || (opt.atom = has['='])) }
            // }
        }
        // if((tmp = get['%']) || opt.limit){
        //     opt.limit = (tmp <= (opt.pack || (1000 * 100)))? tmp : 1;
        // }
        radata(key, function(err, data, o){
            if(err)console.log('ERROR: ',err)
            if(data){
                if(typeof data !== 'string'){
                    if(opt.atom){
                        data = u;
                    } else {
                        Radix.map(data, each) 
                    }
                }
                if(!graph && data){ each(data, '') }
            }
            cb.call(this,graph)
            //gun._.on('in', {'@': id, put: graph, err: err? err : u, rad: Radix});
        }, opt);
        function each(val, has, a,b){
            if(!val){ return }
            has = (key+has).split(esc);
            var soul = has.slice(0,1)[0];
            has = has.slice(-1)[0];
            opt.count = (opt.count || 0) + val.length;
            tmp = val.lastIndexOf('>');
            var state = Radisk.decode(val.slice(tmp+1), null, esc);
            val = Radisk.decode(val.slice(0,tmp), null, esc);
            (graph = graph || {})[soul] = Gun.state.ify(graph[soul], has, state, val, soul);
            if(opt.limit && opt.limit <= opt.count){ return true }
        }
        
    }
    function testPermissions(permsObj,opType){
        let {owner,create,read,update,destroy,chp} = permsObj
        //opType should be one of ['create','read','update','destroy']
        let grp = permsObj[opType]
        if(grp === undefined)isAdmin('Cannot find permissions for this operation!')
        if(grp === null && owner === pub && verified){
            to.next(msg)
        }else if(reqType === 'row' && traverse){
            isMember(grp,function(valid){
                if(valid){
                    traverseNext()
                }else if(owner === pub && verified){
                    traverseNext()
                }else{//admin can do whatever, no need to recur, if not admin, acks err
                    isAdmin()
                }
            })
        }else{
            isMember(grp,function(valid){
                if(valid){
                    to.next(msg)
                }else if(owner === pub && verified){
                    to.next(msg)
                }else{
                    isAdmin()
                }
            })
        }

    }
    function isGrpOwner(){
        let groupName = soul.split('|')[1].split('group/')[1]
        let isRow = /[^\/]+\/t[0-9]+\/r[^|]*/.test(groupName)
        if(isRow){
            let rowPermSoul = groupName + '|permissions'
            getSoul(rowPermSoul,'owner',false,function(message,eve){
                eve.off()
                if(message.put){
                    if(message.put === pub){//is Owner
                        to.next(msg)
                    }else{
                        isAdmin()
                    }
                }else{
                    isAdmin('No permission node found!')
                }
            })
        }else{
            isAdmin('Must be admin to make change to this group')
        }
        
    }
    function isMember(groupName,cb){
        if(groupName === 'ANY'){
            cb.call(this,true)
            return
        }
        let gsoul = base+'|group/'+groupName
        getSoul(gsoul,pub,false,function(val){
            cb.call(this,val)
        })
    }
    function isAdmin(errMsg){
        errMsg = errMsg || op+' PERMISSION DENIED on: '+JSON.stringify(msg)
        if(!verified){
            console.log('PERMISSION DENIED User not verified! OP: ',op,' ON SOUL: ',soul)
            root.on('in',{'@': msg['#']||msg['@'], err: errMsg})
            return
        }
        let [base] = path.split('/')
        getSoul(base+'|group/admin',pub,true, function(val){
            if(val){
                to.next(msg)
                //console.log('An Admin is performing action')
            }else{
                isSuper(errMsg)
            }
        })

    }
    function isSuper(errMsg){
        errMsg = errMsg || op+' PERMISSION DENIED on: '+JSON.stringify(msg)
        getSoul(base+'|super',pub,true, function(val){
            if(val){
                to.next(msg)
                //console.log('Super is performing action')
            }else{
                console.log(errMsg)
                root.on('in',{'@': msg['#']||msg['@'], err: errMsg})
            }
        })
    }
    function addRemoveMember(put){
        let {add,remove} = put
        let ops = Object.values(msg.put[soul])
        let adding = ops.includes(true)
        let removing = ops.includes(false)
        if(adding && removing){
            isMember(add,function(valid){
                if(valid){
                    isMember(remove,function(valid){
                        if(valid){
                            to.next(msg)
                        }else{
                            isGrpOwner()
                        }
                    })
                }else{
                    isGrpOwner()
                }
            })

        }else if(removing){
            isMember(remove,function(valid){
                if(valid){
                    to.next(msg)
                }else{
                    isGrpOwner()
                }
            })
        }else if(adding){
            isMember(add,function(valid){
                if(valid){
                    to.next(msg)
                }else{
                    isGrpOwner()
                }
            })
        }
    }
    function attemptCHP(perms, type){
        console.log('ATTEMPTING TO CHANGE PERMISSIONS')
        let {owner,chp} = perms
        let putKeys = Object.keys(msg.put[soul])
        let needsOwner = putKeys.includes('chp')
        let row = false
        if(type ==='row'){
            needsOwner = (putKeys.includes('chp') || putKeys.includes('owner')) //changing ownership or chp
            row = true
        }
        if(chp === 'ANY'){//not sure when this would be... Anyone could change who could CRUD.
            if(!needsOwner){//cannot change 'chp' unless you own the row (if not row, need admin)
                console.log('`ANY` is editing permissions')
                to.next(msg)
            }else if(needsOwner){
                isOwner()
            }else{
                isAdmin('Invalid permission change, `any` cannot edit owner or group permission settings')
            }
        }else if(verified && pub && chp !== null){//if in group, can edit
            let groupSoul = base+'|group/'+chp
            getSoul(groupSoul,false, true, function(data){//must do lookLocal in case group is referencing itself.
                if(data[pub]){
                    if(!needsOwner){//is on list, can edit
                        to.next(msg)
                    }else if(needsOwner){
                       isOwner()
                    }
                }else{
                    isOwner()
                    //isAdmin('Cannot find a list of group members for group specified in permissions!') //if no group list? or emit a different error message?
                }
            })
        }else if(verified && pub && chp === null){
            isOwner()
        }else{//admins or super can change permissions regardless of permission settings
            isAdmin()
        }
        function isOwner(){
            if(row && pub === owner){//is this the owner of the row
                to.next(msg)
            }else if(!row){
                isGrpOwner()
            }else{
                isAdmin()
            }
        }
    }
    function traverseNext(){
        let pval = hasNext[0] //should be single 'next' column
        let links = path + '/links/'+ pval
        getSoul(links,false,false,function(val){
            if(val){
                for (const nextLink in val) {
                    const valid = val[nextLink];
                    if(valid){//take first valid link, should only be one
                        testRequest(root,request,nextLink)
                    }
                }
            }
        })
    }
    function getSoul(soul,prop,local,cb){
        //local = true; Will ignore cache and always get from disk? Maybe always check cache first?
        //console.log('GETTING SOUL ', soul, prop ,local,permCache[soul])
        //console.log('getting ', soul,' from...')
        if(!(cb instanceof Function)) cb = function(){}
        if(permCache[soul] !== undefined){//null if node does not exist, but has been queried and sub is set
            //console.log('cache')
            if(prop){
                cb.call(this, getValue([soul,prop],permCache)) 
            }else{
                cb.call(this, permCache[soul]) 
            }
        }else if(local){//local could have been cached from a previous local:false get
            //do no setup sub or ask gun, because we might need to know if it is a 'create' vs 'update', ie: super
            //console.log('disk')
            lookLocal(soul,prop||false,function(node){
                let obj = node || {}
                let out
                if(prop){
                    out = getValue([soul,prop],obj)
                }else{
                    out = getValue([soul],obj)
                }
                if(!node){//no data, null to avoid a disk read next time.
                    permCache[soul] = null
                }else{
                    permCache[soul] = out
                }
                addSub(soul) //add sub to update cache to avoid a slow disk read
                cb.call(this,out) 
            })
        }else{
            //console.log('gun')
            let get = {'#':soul}
            if(prop){
                get['.'] = prop
            }
            gun._.on('in', {//faster than .get(function(msg,eve){...????
                get,
                '#': gun._.ask(function(msg){
                    cb.call(this,msg.put && msg.put[soul] || undefined)
                    permCache[soul] = msg.put && msg.put[soul] || null //non-undefined in case no data, but still falsy
                })
            })
            // gun.get(soul).get(function(messg,eve){//check existence
            //     eve.off()
            //     cb.call(this,messg.put)
            //     permCache[soul] = messg.put || null //non-undefined in case no data, but still falsy
            // })
            addSub(soul)
        }
    }
    function addSub(soul){//if you get a local, and it already exists, subscribe and put it in the cache
        gun.get(soul).on(function(data){//setup sub to keep cache accurate
            permCache[soul] = data
        })
    }
}

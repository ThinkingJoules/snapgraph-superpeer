const fs = require('fs');
const config = { port: process.env.OPENSHIFT_NODEJS_PORT || process.env.VCAP_APP_PORT || process.env.PORT || process.argv[2] || 8765 };
const Gun = require('gun');
require('gun/sea')
require('gun/nts')


if(process.env.HTTPS_KEY){
    config.key = fs.readFileSync(process.env.HTTPS_KEY);
    config.cert = fs.readFileSync(process.env.HTTPS_CERT);
    config.server = require('https').createServer(config, Gun.serve(__dirname));
} else {
    config.server = require('http').createServer(Gun.serve(__dirname));
}

//add hooks, listeners here

const DEFAULT_CONFIG = {
    path: __dirname + '/lmdb',
    mapSize: 2*1024*1024*1024
}

const {toGun} = require('./src/toGun')
const {LMDB} = require('./src/lmdb-cmds')
const lmdb = new LMDB(DEFAULT_CONFIG)
toGun(Gun,lmdb)
global.Gun = Gun


const NULL = String.fromCharCode(0)
const IS_STRINGIFY = String.fromCharCode(1)

const GUN_NODE = String.fromCharCode(17)
const IS_DATA = String.fromCharCode(18)

const RS = String.fromCharCode(30)
const ESC = String.fromCharCode(27)

let put = {abc:true,123:true,'_':{'>':{abc:123,123:123}}}
const ENCODING = 'utf16le'



let pvals = Buffer.alloc(0)//p collection so we can delete all things if needed
let newP = ''
let ps = ['abc',123,'def','xyz',123421341242]
for (const p of ps) {
  
    if(!pvals.includes(Buffer.from(String(p),ENCODING)))newP+=String(p)+RS
}
let pBuffer = Buffer.from(pvals.toString(ENCODING)+newP+'\0',ENCODING)

let props = []

let i = 0
//console.log(raw,Buffer.from(RS,ENCODING))
while(true){
    let j = pBuffer.indexOf(RS,i+1,ENCODING)
    //console.log({j})
    if(j-i < 0)break
    props.push(pBuffer.toString(ENCODING,i,j))
    i = j+2//1 for uft8, 2 for utf16le?
}
let soul = 'soul'
// for (let k = 0; k < ps.length; k++) {
//     const origP = ps[k];
//     const parseP = props[k]
//     let oBuf = makeKey(soul,origP)
//     let pBuf = makeKey(soul,parseP)
//     console.log(oBuf.equals(pBuf))
// }




// function makeKey(soul,prop){
//     prop = String(prop)
//     console.log('MAKING',soul,prop)
//     if(prop !== undefined){
//         let firstFour = Buffer.from(prop,ENCODING).subarray(0,4)
//         let lastFour = Buffer.from(prop,ENCODING).subarray(-4)
//         console.log({firstFour,lastFour})
//     }
//     if(prop)return Buffer.from(soul+RS+prop,ENCODING)
//     else return Buffer.from(soul,ENCODING)
// }










let opts = {
    web: config.server.listen(config.port),
    localStorage:false,
    radisk:false,
    multicast:false,
    ws:{path:'/snap'}
}
const gun = Gun(opts);
//setInterval(peers,5000)
// function peers(){
//   console.log('Peers: '+ Object.keys(gun._.opt.peers).length)
// }

console.log('Snapgraph peer started on ' + config.port + ' with /snap');
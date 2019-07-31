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
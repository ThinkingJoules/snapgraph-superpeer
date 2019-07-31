let lmdb = require('node-lmdb');



function LMDB(config){
    this.env = new lmdb.Env()
    const self = this

    self.env.open(config)
    this.dbi = self.env.openDbi({
        name: 'gun-nodes',
        create: true
    })
    this.get = function(soul){
        let txn = self.env.beginTxn()
        let value = txn.getString(self.dbi,soul)
        txn.commit()
        return JSON.parse(value)
    }
    this.write = function(soul,put){
        let txn = self.env.beginTxn()
        let value = Object.assign(JSON.parse(txn.getString(self.dbi,soul) || "{}"),put)
        let val = JSON.stringify(value)
        txn.putString(self.dbi,soul,val)
        txn.commit()
        return true
    }
}
module.exports = {LMDB}
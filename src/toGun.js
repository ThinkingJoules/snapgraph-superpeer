const toGun = (Gun,lmdb) =>{
    Gun.on('create', function (root) {
        root.on('get', function (request) {
            this.to.next(request);
            if (!request)
                return;
            const msgID = request['#'];
            const get = request.get;
            const soul = get['#'];
            const prop = get['.'];
            let value = lmdb.get(soul)
            let error = (value instanceof Error) ? value : false
            if (error) {
                console.error('error', err.stack || err);
                root.on('in', {
                    '@': msgID,
                    put: null,
                    err
                });
            }
            else {
                root.on('in', {
                    '@': msgID,
                    put: value ? { [soul]: value } : null,
                    err: null
                });
            }
        });
        root.on('put', function (request) {
            if (!request)
                return this.to.next(request);
            const msgID = request['#'];
            let soul = Object.keys(request.put)[0]
            let put = request.put[soul]
            let success = lmdb.write(soul,put)
            if(!success){
                root.on('in', {
                    '@': msgID,
                    ok: false,
                    err: true
                });
            }else{
                root.on('in', {
                    '@': msgID,
                    ok: true,
                    err: null
                });  
            }
            this.to.next(request);
        });
        this.to.next(root);
    });
}
module.exports = {toGun}
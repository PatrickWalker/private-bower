var logger = require('../../infrastructure/logger');

//This should come out into the adapter logic
var ssdb = require('ssdb');
var pool;
var conn;

module.exports = function PackageStore() {
    var self = {
        start: _start,

        packages: {},

        getPackage: _getPackage,
        registerPackages: _registerPackages,
        removePackages: _removePackages,

        searchPackage: _searchPackage,
        persistPackage: _persistPackage,
        persistPackages: _persistPackages
    };
    
    var _options;

    function _start(options) {
        _options = options;
        //we will pass the persistenceConfig option into a method which will then set our persistence adapter
        _createSSDBConnection(options.persistenceConfig);
        _loadPackages();
    }

    function _createSSDBConnection(config)
    {
        var ssdbConfig = config.target.split(":");
        pool = ssdb.createPool({
            host: ssdbConfig[0],
            port: ssdbConfig[1]
            });
        conn = pool.acquire();
    }

    function _getPackage(packageName) {
        var item = self.packages[packageName];

        if(!item) {
            return null;
        }

        item.name = packageName;
        item.hits = item.hits || 0;
        item.hits++;

        //could use yield
        setTimeout(_persistPackage(packageName,item), 10)
        return item;
    }

    function _registerPackages(register) {
        for(var i = 0, len = register.length; i < len; i++) {
            var registerPackage = register[i];

            if(!registerPackage.name) {
                logger.log('Undefined package name');

                continue;
            }
            
            var packInfo = {
                name: registerPackage.name,
                url: registerPackage.url,
                hits: 0
            };
            self.packages[registerPackage.name] = packInfo;

            logger.log('Registered package: ' + registerPackage.name);
            _persistPackage(registerPackage.name, packInfo);
        }

    }

    function _removePackages(remove) {
        for(var i = 0, len = remove.length; i < len; i++) {
            delete self.packages[remove[i]];
            _persistRemoval(remove[i])
            logger.log('Removed package: ' + remove[i]);
        }
        
    }


    function _persistRemoval(key)
    {
        conn.del(key, function(err,data){
            if(err)
            {
                throw err;
            }
        });
    }

    function _persistPackage(key,package) {
                var pack = JSON.stringify(package, null, '    '); 
                conn.set(key, pack,function(err, data) {
                    if (err) {
                        throw err;
                    }
                    // data => '1'
                    });
            
    }

    function _persistPackages() {

        for(var key in self.packages) {
                var pack = JSON.stringify(self.packages[key], null, '    '); 
                conn.set(key, pack,function(err, data) {
                    if (err) {
                        throw err;
                    }
                    // data => '1'
                    });
            }
    }

    function _loadPackages() {

        try{
            var b =  conn.keys("","",-1, function(err, data)
            {
                if(err)
                {
                    throw err;
                }
                
                var loadedPackages = {};
                for (i = 0; i < data.length; i++) {
                       conn.get(data[i], function(err,dbPack){
                            try {
                                   dbPack = JSON.parse(dbPack);
                                    
                                    loadedPackages[dbPack.name]= {
                                        name: dbPack.name,
                                        url: dbPack.repo || dbPack.url,
                                        hits: dbPack.hits
                                    }
                            }
                            catch(e) {
                                logger.error("Malformed Entry for " + data[i] + " so skipping it");
                            }
                    
                    if(err)
                    {
                        throw err;
                    }
                    });
                
            }   
                 self.packages = loadedPackages;
        } );
           
        }
        catch(e)
        {
            logger.error("Problem reading information from SSDB");
            throw e;
        }

    }

    function _searchPackage(name) {
        var searchName = name.toLowerCase();
        var packages = [];

        for(var packageName in self.packages) {
            if(self.packages.hasOwnProperty(packageName) &&
                packageName.toLowerCase().indexOf(searchName) !== -1) {

                var item = self.packages[packageName];
                packages.push({
                    name: item.name,
                    url: item.url
                });
            }
        }

        return packages;
    }

    return self;
}();
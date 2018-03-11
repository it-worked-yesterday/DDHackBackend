/**
 * Hackathon example server
 * Allows getting logs from CrateDB or RethinkDB using:
 * HTTP GET /logs/cratedb?min=etc&max=etc
 * or HTTP GET /logs/rethinkdb?min=etc&max=etc
 *
 * Feel free to modify this code however you want, or delete
 * it and start over from scratch.
 */

require('dotenv/config');
const nconf = require('nconf');
const Koa = require('koa');
var bodyParser = require('koa-bodyparser');
const Router = require('koa-router');
const route = require('koa-route');
const crate = require('node-crate');
const logger = require('koa-logger');
const rethinkdbdash = require('rethinkdbdash');
const moment = require('moment');
const { Readable, Transform, PassThrough } = require('stream');
const AggregateResponseTime = require('./src/AggregateResponseTime');
const websockify = require('koa-websocket');
//var cors = require('koa-cors');

 



const max_limit = 50000;

var count = 0;
var resTimeAggTransform = AggregateResponseTime();
const passThrough = new PassThrough();

// Initialize configuration variables
nconf
    .argv({ parseValues: true })
    .env({ parseValues: true, lowerCase: true })
    .defaults({
        rethink_database: 'hackathon',
        rethink_host: "databases-internal.hackathon.venom360.com",
        rethink_port: 28015,
        crate_host: "databases-internal.hackathon.venom360.com",
        crate_port: 4200,
        app_port: 8080
    })
    .required([
        'rethink_database',
        'rethink_host',
        'rethink_port',
        'crate_host',
        'crate_port',
        'app_port'
    ]);



// GEN PUPOSE METHODS
//get last 1 min data 
async function getLatestData() {
    const minDate = moment.utc(moment.utc().subtract(1, "minutes").toISOString(), moment.ISO_8601);
    const maxDate = moment.utc(moment.utc().toISOString(), moment.ISO_8601);
    var entries;
    try {
        console.log("gettting latest data ");
        entries = await r
            .table('logs')
            .between(minDate.toDate(), maxDate.toDate(), { index: 'time' })
            .limit(max_limit)
            .run();
        return entries;
    } catch (error) {
        return error;
    }
};

const sse = (event, data) => {
    return `event:${ event }\ndata: ${ data }\n\n`
  }

// Read Data Stream
const latestDataStream = new Readable({
    async read(size) {
        console.log("into read ");
        count = count + 1;
        try {
            setInterval(async () => {
                data = await getLatestData();
                console.log("abc");
                this.push(data)
            }, 60000);

        } catch (error) {
            console.log("error", error);
        }
        if (count >= 5) {
            clearInterval();
        }


    }
});



// Connect to databases
const r = rethinkdbdash({
    db: nconf.get('rethink_database'),
    servers: [
        { host: nconf.get('rethink_host'), port: nconf.get('rethink_port') }
    ],
    ssl: { rejectUnauthorized: false }
});

//crate.connect(nconf.get('crate_host'), nconf.get('crate_port'));

// Start web server using Koa
const app = new Koa();
const router = new Router();
const websocket = websockify(app);

app.use(bodyParser());
app.use(logger());
//app.use(cors());

// HTTP GET /logs/rethinkdb?min=etc&max=etc to get logs between dates
router.get('/logs/rethinkdb', async ctx => {
    const { min, max } = ctx.query;
    if (!min || !max)
        ctx.throw(400, 'Must specify min and   max in query string.');

    const minDate = moment.utc(min, moment.ISO_8601);
    const maxDate = moment.utc(max, moment.ISO_8601);

    if (!minDate.isValid() || !maxDate.isValid())
        ctx.throw(400, 'Min and max must be ISO 8601 date strings');

    const entries = await r
        .table('logs')
        .between(minDate.toDate(), maxDate.toDate(), { index: 'time' })
        .limit(max_limit)
        .run();

    ctx.status = 200;
    ctx.body = entries;
});

// HTTP GET /logs/rethinkdb?min=etc&max=etc to get logs between dates
router.post('/logs/rethinkdb', async ctx => {
    console.log('data is', ctx.request.body);
    const { id } = ctx.request.body;
    if (!id) {
        ctx.throw(400, "empty body sent");
    }
    var entries;
    try {
        entries = await r
            .table('logs')
            .filter({ id: id })
            .limit(max_limit)
            .run();
    } catch (error) {
        console.log("error occured:", error);
    }

    ctx.status = 200;
    ctx.body = entries;
});


// Using routes
websocket.ws.use(route.all('/logs/rethinkdb/getResponseTimes', function (ctx) {
    ctx.websocket.send('Websocket well connected');

    const send = (data) => {
        let { type, msg } = data; // or whatever :)
        ctx.websocket.send(data);
      }
      
      passThrough.on('data', send);
      ctx.req.on('close', ctx.res.end());
      ctx.req.on('finish', ctx.res.end());
      ctx.req.on('error', ctx.res.end());
      ctx.type = 'text/event-stream';
      ctx.body = data;
      ctx.status = 200;

  }));
  websocket.listen(8518);


// HTTP GET streams the aggregate of last 1 min response time of overall service
router.get('/logs/rethinkdb/getResponseTimes', async ctx => {
    
        const send = (data) => {
            let { type, msg } = data; // or whatever :)
            passThrough.write(data);
          }
          
          passThrough.on('data', send);
          ctx.req.on('close', ctx.res.end());
          ctx.req.on('finish', ctx.res.end());
          ctx.req.on('error', ctx.res.end());
          ctx.type = 'text/event-stream';
          ctx.body = passThrough;
    });

    // HTTP GET rethink/stream
router.get('/logs/rethinkdb/stream', async ctx => {
    var entries;
    try {
        entries = await getLatestData();
    } catch (error) {
        console.log("error occured:", error);
    }

    console.log("entries are ", entries);
    ctx.status = 200;
    ctx.body = entries;
});

    

// Use router middleware
app.use(router.routes());
app.use(router.allowedMethods());


// Start server on app port.
const port = nconf.get('app_port');
app.listen(port, () => {
    console.log(`Server started on port ${port}.`);

});

// the best part of the server code
latestDataStream
    .pipe(resTimeAggTransform)
    .pipe(passThrough);














/* THEM APPLES

// HTTP GET rethink/stream
router.get('/logs/rethinkdb/stream', async ctx => {
    var entries;
    try {
        entries = await getLatestData();
    } catch (error) {
        console.log("error occured:", error);
    }

    console.log("entries are ", entries);
    ctx.status = 200;
    ctx.body = entries;
});


// HTTP GET /logs/cratedb?min=etc&max=etc to get logs between dates
router.get('/logs/cratedb', async ctx => {
    const { min, max } = ctx.query;
    if (!min || !max)
        ctx.throw(400, 'Must specify min and max in query string.');

    const minDate = moment.utc(min, moment.ISO_8601);
    const maxDate = moment.utc(max, moment.ISO_8601);

    if (!minDate.isValid() || !maxDate.isValid())
        ctx.throw(400, 'Min and max must be ISO 8601 date strings');

    const entries = await crate.execute(
        'SELECT * FROM logs WHERE time BETWEEN ? AND ? LIMIT ?',
        [minDate.toDate(), maxDate.toDate(), max_limit]
    );

    ctx.status = 200;
    ctx.body = entries.json;
});

*/
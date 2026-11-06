// npm hyperblobs writer for tests/server.rs, served over a local HyperDHT testnet.
// node blobs.js <dir>
// Prints "bootstrap <port>", then one JSON line {key, blobs: [{id, link}]}, then "ready".
// Exits when stdin closes.
const fs = require('fs')
const c = require('compact-encoding')
const z32 = require('z32')
const HypercoreId = require('hypercore-id-encoding')
const Hypercore = require('hypercore')
const Hyperblobs = require('hyperblobs')
const Hyperswarm = require('hyperswarm')
const createTestnet = require('hyperdht/testnet')

const log = (s) => process.stdout.write(s + '\n')
process.stdin.on('end', () => process.exit(0))
process.stdin.resume()

// Byte j of blob b; tests/server.rs has the same function.
function blob(b, len) {
  const out = Buffer.alloc(len)
  for (let j = 0; j < len; j++) out[j] = ((j * 7 + b * 13) ^ (j >>> 9)) & 0xff
  return out
}

const blobId = {
  preencode(state, b) {
    c.uint.preencode(state, b.blockOffset)
    c.uint.preencode(state, b.blockLength)
    c.uint.preencode(state, b.byteOffset)
    c.uint.preencode(state, b.byteLength)
  },
  encode(state, b) {
    c.uint.encode(state, b.blockOffset)
    c.uint.encode(state, b.blockLength)
    c.uint.encode(state, b.byteOffset)
    c.uint.encode(state, b.byteLength)
  },
  decode(state) {
    return { blockOffset: c.uint.decode(state), blockLength: c.uint.decode(state), byteOffset: c.uint.decode(state), byteLength: c.uint.decode(state) }
  }
}

async function main() {
  const [dir] = process.argv.slice(2)
  fs.rmSync(dir, { recursive: true, force: true })
  const testnet = await createTestnet(3, { host: '127.0.0.1' })
  log('bootstrap ' + testnet.bootstrap[0].port)

  const core = new Hypercore(dir)
  await core.ready()
  const blobs = new Hyperblobs(core, { blockSize: 512 * 1024 })
  const ids = []
  // 0: uniform 512 KiB blocks plus a short tail, as PearTube stores videos.
  ids.push(await blobs.put(blob(0, 5 * 512 * 1024 + 123)))
  // 1: a write stream with uneven chunks, one block each.
  const ws = blobs.createWriteStream()
  const data = blob(1, 300000)
  const done = new Promise((resolve, reject) => { ws.once('close', resolve); ws.once('error', reject) })
  let off = 0
  for (let i = 0; off < data.length; i++) {
    const len = Math.min(data.length - off, 1 + ((i * 7919) % 70000))
    ws.write(data.subarray(off, off + len))
    off += len
  }
  ws.end()
  await done
  ids.push(ws.id)
  // 2: empty. 3: one small block.
  ids.push(await blobs.put(Buffer.alloc(0)))
  ids.push(await blobs.put(blob(3, 1000)))

  const swarm = new Hyperswarm({ bootstrap: testnet.bootstrap })
  swarm.on('connection', (conn) => core.replicate(conn))
  await swarm.join(core.discoveryKey, { server: true, client: false }).flushed()
  log(JSON.stringify({
    key: HypercoreId.encode(core.key),
    discoveryKey: core.discoveryKey.toString('hex'),
    blobs: ids.map((id) => ({ id, z32: z32.encode(c.encode(blobId, id)) }))
  }))
  log('ready')
}

main().catch((err) => {
  log('error ' + err.stack)
  process.exit(1)
})

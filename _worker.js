import { connect } from 'cloudflare:sockets';

// PENGATURAN UTAMA
const userID = '2c57fa56-5192-4f7e-978b-2c14cc86aa6e';
const proxyIP = '104.17.2.1'; // IP Jakarta

export default {
  async fetch(request, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response('VLESS Running', { status: 200 });
      }
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response(err.toString());
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

  let remoteSocketWapper = { value: null };

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const vlessHeader = new Uint8Array(chunk.slice(0, 24));
      // Sederhananya, kita langsung proses TCP outbound
      const addressType = new Uint8Array(chunk.slice(20, 21))[0];
      let addressRemote = '';
      let addressIndex = 21;

      if (addressType === 1) { // IPv4
        addressRemote = new Uint8Array(chunk.slice(22, 26)).join('.');
        addressIndex = 26;
      } else if (addressType === 2) { // Domain
        const len = new Uint8Array(chunk.slice(21, 22))[0];
        addressRemote = new TextDecoder().decode(chunk.slice(22, 22 + len));
        addressIndex = 22 + len;
      }

      const portRemote = new DataView(chunk.slice(addressIndex, addressIndex + 2)).getUint16(0);
      const rawClientData = chunk.slice(addressIndex + 2);

      const tcpSocket = connect({ hostname: addressRemote, port: portRemote });
      remoteSocketWapper.value = tcpSocket;

      const writer = tcpSocket.writable.getWriter();
      await writer.write(rawClientData);
      writer.releaseLock();

      tcpSocket.readable.pipeTo(new WritableStream({
        write(chunk) {
          webSocket.send(chunk);
        }
      }));
    }
  })).catch(() => {});

  return new Response(null, { status: 101, webSocket: client });
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => controller.enqueue(event.data));
      webSocketServer.addEventListener('close', () => controller.close());
      webSocketServer.addEventListener('error', (err) => controller.error(err));
    }
  });
}

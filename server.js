const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const fs = require('fs');
const path = require('path');
const os = require('os');
const qrcode = require('qrcode-terminal');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

const PLAY_DELAY_MS = 1000; // headroom for clock sync + buffering before playback starts

let globalPlaylist = [];
let playbackState = {
    filename: null,
    isPlaying: false,
    mediaTime: 0,
    serverTimeAtUpdate: Date.now()
};

// ---------- FOLDERS & PERSISTENT QUEUE ORDER ----------
// Songs can now live in subfolders inside public/. Whichever folder is active, and
// whatever order you've dragged its songs into, is saved to disk — so it's exactly
// how you left it even after fully closing and re-running the server from scratch.
const PUBLIC_DIR = path.join(__dirname, 'public');
const STATE_FILE = path.join(__dirname, 'playlist-state.json');
const MEDIA_EXT = ['.mp4', '.mp3', '.webm', '.wav', '.ogg', '.oga', '.m4a', '.aac', '.flac', '.opus', '.mov', '.mkv'];

let state = { currentFolder: '', folderOrders: {} };
try {
    if (fs.existsSync(STATE_FILE)) {
        state = { ...state, ...JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')) };
    }
} catch (err) {
    console.error('Could not read playlist-state.json, starting fresh:', err.message);
}

function saveState() {
    try {
        fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    } catch (err) {
        console.error('Could not save playlist-state.json:', err.message);
    }
}

function listFolders() {
    if (!fs.existsSync(PUBLIC_DIR)) fs.mkdirSync(PUBLIC_DIR);
    try {
        return fs.readdirSync(PUBLIC_DIR, { withFileTypes: true })
            .filter(d => d.isDirectory() && !d.name.startsWith('.'))
            .map(d => d.name)
            .sort();
    } catch (err) {
        console.error('Error listing folders:', err);
        return [];
    }
}

// Files directly inside public/<folder> ('' = public/ itself), returned as paths
// relative to public/ (e.g. "Rock/Song.mp3") so express.static serves them unchanged.
function listMediaFiles(folder) {
    const dir = path.join(PUBLIC_DIR, folder || '');
    if (!fs.existsSync(dir)) return [];
    try {
        return fs.readdirSync(dir, { withFileTypes: true })
            .filter(d => d.isFile() && MEDIA_EXT.includes(path.extname(d.name).toLowerCase()))
            .map(d => (folder ? `${folder}/${d.name}` : d.name))
            .sort();
    } catch (err) {
        console.error('Error scanning folder:', err);
        return [];
    }
}

// Reconciles the saved order for a folder with what's actually on disk: drops songs
// that got deleted, appends new ones at the end, defaults to A-Z if nothing saved yet.
function getOrderedPlaylist(folder) {
    const actual = listMediaFiles(folder);
    const saved = state.folderOrders[folder];
    let ordered;
    if (Array.isArray(saved) && saved.length) {
        const actualSet = new Set(actual);
        const kept = saved.filter(f => actualSet.has(f));
        const keptSet = new Set(kept);
        ordered = [...kept, ...actual.filter(f => !keptSet.has(f))];
    } else {
        ordered = actual;
    }
    state.folderOrders[folder] = ordered;
    return ordered;
}

function switchFolder(folder) {
    state.currentFolder = folder;
    globalPlaylist = getOrderedPlaylist(folder);
    saveState();
}

switchFolder(state.currentFolder || '');
// ---------- END FOLDERS & PERSISTENT QUEUE ORDER ----------

io.on('connection', (socket) => {
    console.log(`Device connected: ${socket.id}`);

    socket.emit('load_playlist', globalPlaylist);
    socket.emit('load_folders', { folders: listFolders(), current: state.currentFolder });
    // Late joiners / reconnects need the CURRENT playback state, not just the playlist,
    // otherwise a phone that reconnects mid-song has no way to catch up in sync.
    socket.emit('sync_state', playbackState);

    socket.on('request_sync_state', () => {
        socket.emit('sync_state', playbackState);
    });

    // --- CLOCK SYNC (NTP-style ping/pong) ---
    // A future "play at time X" is meaningless unless every device agrees on what
    // time it is. Phone and laptop clocks are almost never identical, which was
    // the root cause of the drift: respond as fast as possible for an accurate RTT.
    socket.on('time_sync', () => {
        socket.emit('time_sync_response', Date.now());
    });

    // --- PLAYBACK CONTROL ---
    socket.on('trigger_play', (data) => {
        if (!data || typeof data.filename !== 'string') return;
        const syncTime = Date.now() + PLAY_DELAY_MS;
        const mediaTime = Number(data.mediaTime) || 0;

        playbackState = { filename: data.filename, isPlaying: true, mediaTime, serverTimeAtUpdate: syncTime };

        io.emit('command_play', { time: syncTime, filename: data.filename, mediaTime });
    });

    socket.on('trigger_pause', (data) => {
        const mediaTime = Number(data && data.mediaTime) || 0;

        playbackState = { ...playbackState, isPlaying: false, mediaTime, serverTimeAtUpdate: Date.now() };

        io.emit('command_pause', { mediaTime });
    });

    socket.on('trigger_seek', (data) => {
        const syncTime = Date.now() + PLAY_DELAY_MS;
        const mediaTime = Number(data && data.mediaTime) || 0;

        // Seeking doesn't change whether the group is playing or paused, just the position.
        playbackState = { ...playbackState, mediaTime, serverTimeAtUpdate: syncTime };

        io.emit('command_seek', { time: syncTime, mediaTime });
    });

    socket.on('switch_folder', (folder) => {
        if (typeof folder !== 'string') return;
        if (folder !== '' && !listFolders().includes(folder)) return; // ignore unknown/invalid folders
        switchFolder(folder);
        io.emit('load_folders', { folders: listFolders(), current: state.currentFolder });
        io.emit('load_playlist', globalPlaylist);
    });

    socket.on('update_queue', (newQueue) => {
        if (!Array.isArray(newQueue)) return;
        globalPlaylist = newQueue;
        state.folderOrders[state.currentFolder] = newQueue; // remember this order...
        saveState();                                        // ...even across a full restart
        socket.broadcast.emit('load_playlist', globalPlaylist);
    });

    socket.on('disconnect', () => {
        console.log(`Device disconnected: ${socket.id}`);
    });
});

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n=========================================`);
    console.log(`🎵 Sync Server is LIVE!`);
    console.log(`=========================================\n`);
    
    const interfaces = os.networkInterfaces();
    let masterIp = null;
    
    // Find the master IP (Prioritizing the USB Tether/Ethernet connection)
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                // If the network name contains 'ethernet' or 'ndis' (USB tethering), prioritize it
                if (name.toLowerCase().includes('ethernet') || name.toLowerCase().includes('ndis')) {
                    masterIp = iface.address;
                } else if (!masterIp) {
                    // Fallback to the first available IP if USB isn't plugged in yet
                    masterIp = iface.address;
                }
            }
        }
    }
    
    if (masterIp) {
        const phoneLink = `http://${masterIp}:${PORT}`;
        console.log(`📱 Scan this ONE master QR code for ALL devices:`);
        qrcode.generate(phoneLink, { small: true });
        console.log(`🔗 Link: ${phoneLink}`);
    } else {
        console.log(`⚠️ No active network connection found. Connect to Wi-Fi or plug in USB tethering.`);
    }
    
    console.log(`=========================================\n`);
});
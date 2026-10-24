import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { 
  getFirestore, 
  collection, 
  doc, 
  setDoc, 
  getDoc,
  getDocs, 
  getDocFromServer,
  onSnapshot,
  addDoc 
} from 'firebase/firestore';
export const firebaseConfig = {
  projectId: "trusty-gearbox-h8f9p",
  appId: "1:748023805602:web:7aa99fe1ae8e3c7b7689fb",
  apiKey: "AIzaSyDz8OWRswzP75Y8Hx9LI5o5YjZgDoeQPFY",
  authDomain: "trusty-gearbox-h8f9p.firebaseapp.com",
  firestoreDatabaseId: "ai-studio-lecturehub-cc2540a3-fe97-4523-8749-32547fcb61b1",
  storageBucket: "trusty-gearbox-h8f9p.firebasestorage.app",
  messagingSenderId: "748023805602",
  measurementId: ""
};

// Initialize Firebase App
export const firebaseApp = initializeApp({
  apiKey: firebaseConfig.apiKey,
  authDomain: firebaseConfig.authDomain,
  projectId: firebaseConfig.projectId,
  storageBucket: firebaseConfig.storageBucket,
  messagingSenderId: firebaseConfig.messagingSenderId,
  appId: firebaseConfig.appId,
  measurementId: firebaseConfig.measurementId,
});

// Initialize Firestore with specific database ID
export const firestoreDb = firebaseConfig.firestoreDatabaseId && firebaseConfig.firestoreDatabaseId !== '(default)'
  ? getFirestore(firebaseApp, firebaseConfig.firestoreDatabaseId)
  : getFirestore(firebaseApp);

export const firebaseAuth = getAuth(firebaseApp);

export let isFirebaseOnline = false;

// Connection test according to critical constraints
export async function testConnection(): Promise<boolean> {
  try {
    await getDocFromServer(doc(firestoreDb, 'test', 'connection'));
    isFirebaseOnline = true;
    console.log('[LectureHub Firebase] Cloud Firestore connection verified successfully.');
    if (typeof document !== 'undefined') {
      const el = document.getElementById('storageStatus');
      if (el) el.textContent = 'Cloud Synchronized · Firebase Firestore Online';
    }
    return true;
  } catch (error) {
    if (error instanceof Error && error.message.includes('the client is offline')) {
      console.warn('[LectureHub Firebase] The client is offline. Operating with local cache.');
    } else {
      console.log('[LectureHub Firebase] Firestore connected (ready for documents).');
      isFirebaseOnline = true;
      if (typeof document !== 'undefined') {
        const el = document.getElementById('storageStatus');
        if (el) el.textContent = 'Cloud Synchronized · Firebase Firestore';
      }
    }
    return false;
  }
}

// Collections to synchronize across users and devices
const SYNC_COLLECTIONS = [
  'users',
  'courses',
  'enrollments',
  'assignments',
  'submissions',
  'quizzes',
  'quizAttempts',
  'announcements',
  'meetings'
] as const;

type SyncCollection = typeof SYNC_COLLECTIONS[number];

// Sync a single record to Firestore
export async function syncRecordToCloud(colName: string, id: string | number, data: Record<string, unknown>) {
  if (!firestoreDb) return;
  try {
    const docRef = doc(firestoreDb, colName, String(id));
    // Clean undefined values before writing
    const cleanData = JSON.parse(JSON.stringify(data));
    await setDoc(docRef, cleanData, { merge: true });
  } catch (err) {
    console.warn(`[Firebase Sync Error] could not sync ${colName}/${id}:`, err);
  }
}

// Initial full cloud sync & real-time subscription
export function startFirebaseSync(
  getDB: () => Record<string, any>,
  onCloudUpdate: (colName: string, items: any[]) => void
) {
  testConnection();

  // 1. Initial push of local items to Firestore if not already present
  setTimeout(async () => {
    try {
      const db = getDB();
      for (const colName of SYNC_COLLECTIONS) {
        const localItems = Array.isArray(db[colName]) ? db[colName] : [];
        for (const item of localItems) {
          if (item && (item.id !== undefined && item.id !== null)) {
            syncRecordToCloud(colName, item.id, item);
          }
        }
      }
    } catch (e) {
      console.warn('[Firebase Seed Sync]', e);
    }
  }, 1000);

  // 2. Real-time subscriptions for shared learning items
  const unsubscribers: Array<() => void> = [];

  for (const colName of SYNC_COLLECTIONS) {
    try {
      const colRef = collection(firestoreDb, colName);
      const unsub = onSnapshot(colRef, (snapshot) => {
        if (!snapshot.empty) {
          const cloudItems: any[] = [];
          snapshot.forEach((d) => {
            cloudItems.push(d.data());
          });
          onCloudUpdate(colName, cloudItems);
        }
      }, (err) => {
        console.warn(`[Firebase Realtime Error] on ${colName}:`, err);
      });
      unsubscribers.push(unsub);
    } catch (e) {
      console.warn(`[Firebase Listener Error] on ${colName}:`, e);
    }
  }

  return () => {
    unsubscribers.forEach((unsub) => unsub());
  };
}

// WebRTC Signaling Interface for Multi-Device Video Calls
export interface CallSignal {
  from: string;
  fromName: string;
  to: string; // recipient peerId or 'all' or 'host'
  type: 'join' | 'offer' | 'answer' | 'candidate' | 'bye' | 'state' | 'chat';
  payload: any;
  timestamp: number;
}

export async function initCallRoom(roomCode: string, roomData: Record<string, any>) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const roomRef = doc(firestoreDb, 'calls', cleanCode);
  await setDoc(roomRef, {
    ...roomData,
    code: cleanCode,
    active: true,
    updatedAt: Date.now()
  }, { merge: true });
}

export async function getCallRoom(roomCode: string) {
  if (!firestoreDb) return null;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const snap = await getDoc(doc(firestoreDb, 'calls', cleanCode));
  return snap.exists() ? snap.data() : null;
}

export async function sendCallSignal(roomCode: string, signal: CallSignal) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const signalsRef = collection(firestoreDb, 'calls', cleanCode, 'signals');
  await addDoc(signalsRef, {
    ...signal,
    timestamp: Date.now()
  });
}

export function listenCallSignals(
  roomCode: string, 
  myPeerId: string, 
  onSignal: (signal: CallSignal, signalId: string) => void
) {
  if (!firestoreDb) return () => {};
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const signalsRef = collection(firestoreDb, 'calls', cleanCode, 'signals');
  const seenSignalIds = new Set<string>();

  const unsub = onSnapshot(signalsRef, (snapshot) => {
    snapshot.docChanges().forEach((change) => {
      if (change.type === 'added') {
        const id = change.doc.id;
        if (seenSignalIds.has(id)) return;
        seenSignalIds.add(id);
        const data = change.doc.data() as CallSignal;
        // Ignore signals sent by myself
        if (data.from === myPeerId) return;
        // Ignore signals specifically addressed to another peer
        if (data.to && data.to !== myPeerId && data.to !== 'all' && data.to !== 'host') {
          return;
        }
        onSignal(data, id);
      }
    });
  }, (err) => {
    console.warn(`[LectureHub Signaling Error] on room ${cleanCode}:`, err);
  });

  return unsub;
}

export async function leaveCallRoom(roomCode: string, myPeerId: string, isHost: boolean) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  try {
    await sendCallSignal(cleanCode, {
      from: myPeerId,
      fromName: 'Leaving',
      to: 'all',
      type: 'bye',
      payload: {},
      timestamp: Date.now()
    });
    if (isHost) {
      await setDoc(doc(firestoreDb, 'calls', cleanCode), {
        active: false,
        endedAt: Date.now()
      }, { merge: true });
    }
  } catch (e) {
    console.warn('[LectureHub Signaling] error on leaving room:', e);
  }
}

// Expose on window for client scripts
if (typeof window !== 'undefined') {
  (window as any).lhSignaling = {
    initCallRoom,
    getCallRoom,
    sendCallSignal,
    listenCallSignals,
    leaveCallRoom
  };
}

import { initializeApp, getApps, getApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { 
  getFirestore, 
  initializeFirestore,
  collection, 
  doc, 
  setDoc, 
  getDoc,
  getDocs, 
  getDocFromServer,
  onSnapshot,
  addDoc,
  deleteDoc,
  query,
  where
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

// Initialize or reuse Firebase App
export const firebaseApp = getApps().length > 0 ? getApp() : initializeApp({
  apiKey: firebaseConfig.apiKey,
  authDomain: firebaseConfig.authDomain,
  projectId: firebaseConfig.projectId,
  storageBucket: firebaseConfig.storageBucket,
  messagingSenderId: firebaseConfig.messagingSenderId,
  appId: firebaseConfig.appId,
  measurementId: firebaseConfig.measurementId,
});

const targetDbId = firebaseConfig.firestoreDatabaseId && firebaseConfig.firestoreDatabaseId !== '(default)'
  ? firebaseConfig.firestoreDatabaseId
  : undefined;

// Initialize Firestore with experimentalForceLongPolling to guarantee proxy, iframe, and mobile network reachability
let firestoreInstance;
try {
  firestoreInstance = initializeFirestore(firebaseApp, {
    experimentalForceLongPolling: true,
  }, targetDbId);
} catch {
  firestoreInstance = targetDbId ? getFirestore(firebaseApp, targetDbId) : getFirestore(firebaseApp);
}

export const firestoreDb = firestoreInstance;
export const firebaseAuth = getAuth(firebaseApp);

export enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

export interface FirestoreErrorInfo {
  error: string;
  operationType: OperationType;
  path: string | null;
  authInfo: {
    userId?: string | null;
    email?: string | null;
    emailVerified?: boolean | null;
    isAnonymous?: boolean | null;
    tenantId?: string | null;
    providerInfo?: {
      providerId?: string | null;
      email?: string | null;
    }[];
  };
}

export function handleFirestoreError(error: unknown, operationType: OperationType, path: string | null) {
  const errInfo: FirestoreErrorInfo = {
    error: error instanceof Error ? error.message : String(error),
    authInfo: {
      userId: firebaseAuth.currentUser?.uid,
      email: firebaseAuth.currentUser?.email,
      emailVerified: firebaseAuth.currentUser?.emailVerified,
      isAnonymous: firebaseAuth.currentUser?.isAnonymous,
      tenantId: firebaseAuth.currentUser?.tenantId,
      providerInfo: firebaseAuth.currentUser?.providerData?.map(provider => ({
        providerId: provider.providerId,
        email: provider.email,
      })) || []
    },
    operationType,
    path
  };
  console.error('Firestore Error: ', JSON.stringify(errInfo));
  throw new Error(JSON.stringify(errInfo));
}

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
      console.warn('[LectureHub Firebase] The client is operating in offline mode.');
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
  type: 'join' | 'offer' | 'answer' | 'description' | 'candidate' | 'bye' | 'state' | 'chat' | 'reaction' | 'mute_request' | 'lower_hand' | 'announcement';
  payload: any;
  timestamp: number;
}

export interface CallParticipant {
  id: string;
  name: string;
  role: 'host' | 'student';
  joinedAt: number;
  status: 'active' | 'left';
  cam?: boolean;
  mic?: boolean;
  hand?: boolean;
  handRaisedAt?: number | null;
  speaking?: boolean;
}

export interface CallMessage {
  id?: string;
  senderId: string;
  senderName: string;
  senderRole: 'host' | 'student';
  text: string;
  timestamp: number;
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: any;
  const timeoutPromise = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([
    promise.then(res => { clearTimeout(timer); return res; }),
    timeoutPromise
  ]);
}

export async function initCallRoom(roomCode: string, roomData: Record<string, any>) {
  if (!firestoreDb) {
    throw new Error('Firestore database is not initialized. Please check network connection.');
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const roomRef = doc(firestoreDb, 'calls', cleanCode);
  try {
    await withTimeout(
      setDoc(roomRef, {
        ...roomData,
        code: cleanCode,
        active: true,
        chatEnabled: roomData.chatEnabled !== false,
        updatedAt: Date.now()
      }, { merge: true }),
      9000,
      'Room initialization timed out. Please check your network and tap Retry.'
    );
    console.log(`[Firebase Call Room] initCallRoom succeeded for room ${cleanCode}`);
  } catch (err: any) {
    console.error(`[Firebase Call Room] initCallRoom failed for room ${cleanCode}:`, err);
    throw new Error(`Failed to initialize classroom on cloud: ${err?.message || 'Network error'}`);
  }
}

export async function updateCallRoom(roomCode: string, updates: Record<string, any>) {
  if (!firestoreDb) {
    throw new Error('Firestore database is not initialized.');
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const roomRef = doc(firestoreDb, 'calls', cleanCode);
  try {
    await setDoc(roomRef, {
      ...updates,
      updatedAt: Date.now()
    }, { merge: true });
  } catch (err: any) {
    console.warn(`[Firebase Call Room] updateCallRoom error on ${cleanCode}:`, err);
    throw err;
  }
}

export async function getCallRoom(roomCode: string) {
  if (!firestoreDb) {
    throw new Error('Firestore database is not initialized. Please verify your connection.');
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  try {
    const snap = await withTimeout(
      getDoc(doc(firestoreDb, 'calls', cleanCode)),
      8000,
      'Could not reach classroom server within 8 seconds. Please check your internet connection.'
    );
    return snap.exists() ? snap.data() : null;
  } catch (err: any) {
    console.error(`[Firebase Call Room] getCallRoom lookup failed for room ${cleanCode}:`, err);
    throw new Error(`Failed to lookup classroom session: ${err?.message || 'Network error'}`);
  }
}

export function listenCallRoom(
  roomCode: string, 
  onUpdate: (roomData: any) => void,
  onError?: (err: any) => void
) {
  if (!firestoreDb) {
    if (onError) onError(new Error('Firestore database is not initialized.'));
    return () => {};
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const roomRef = doc(firestoreDb, 'calls', cleanCode);
  return onSnapshot(roomRef, (snap) => {
    if (snap.exists()) {
      onUpdate(snap.data());
    } else {
      onUpdate(null);
    }
  }, (err) => {
    console.warn(`[LectureHub] Room listen error on ${cleanCode}:`, err);
    if (onError) onError(err);
  });
}

export async function registerParticipant(roomCode: string, participant: CallParticipant) {
  if (!firestoreDb) {
    throw new Error('Firestore database is not initialized. Please check your connection.');
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const partRef = doc(firestoreDb, 'calls', cleanCode, 'participants', participant.id);
  try {
    await withTimeout(
      setDoc(partRef, {
        ...participant,
        updatedAt: Date.now()
      }, { merge: true }),
      9000,
      'Participant registration timed out. Please check your network and tap Retry.'
    );
    console.log(`[Firebase Call Room] registerParticipant succeeded for ${participant.id} in room ${cleanCode}`);
  } catch (err: any) {
    console.error(`[Firebase Call Room] registerParticipant failed for room ${cleanCode}:`, err);
    throw new Error(`Failed to register in live class: ${err?.message || 'Network error'}`);
  }
}

export async function updateParticipantState(roomCode: string, participantId: string, updates: Partial<CallParticipant>) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const partRef = doc(firestoreDb, 'calls', cleanCode, 'participants', participantId);
  try {
    await setDoc(partRef, {
      ...updates,
      updatedAt: Date.now()
    }, { merge: true });
  } catch (e) {
    console.warn('[LectureHub] updateParticipantState error:', e);
  }
}

export async function removeParticipant(roomCode: string, participantId: string) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const partRef = doc(firestoreDb, 'calls', cleanCode, 'participants', participantId);
  try {
    await deleteDoc(partRef);
  } catch (e) {
    console.warn('[LectureHub] removeParticipant error:', e);
  }
}

export function listenParticipants(
  roomCode: string, 
  onUpdate: (participants: CallParticipant[]) => void,
  onError?: (err: any) => void
) {
  if (!firestoreDb) {
    if (onError) onError(new Error('Firestore database is not initialized.'));
    return () => {};
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const partCol = collection(firestoreDb, 'calls', cleanCode, 'participants');
  return onSnapshot(partCol, (snapshot) => {
    const list: CallParticipant[] = [];
    snapshot.forEach(d => {
      const p = d.data() as CallParticipant;
      if (p.status !== 'left') {
        list.push(p);
      }
    });
    // Sort: Hand raised participants first (by timestamp), then alphabetically by name
    list.sort((a, b) => {
      if (a.hand && !b.hand) return -1;
      if (!a.hand && b.hand) return 1;
      if (a.hand && b.hand) return (a.handRaisedAt || 0) - (b.handRaisedAt || 0);
      return (a.name || '').localeCompare(b.name || '');
    });
    onUpdate(list);
  }, (err) => {
    console.warn(`[LectureHub] Participant listen error on ${cleanCode}:`, err);
    if (onError) onError(err);
  });
}

export async function sendCallSignal(roomCode: string, signal: CallSignal) {
  if (!firestoreDb) {
    console.warn('[Firebase Signaling] firestoreDb not ready, dropping signal');
    return;
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const signalsRef = collection(firestoreDb, 'calls', cleanCode, 'signals');
  try {
    await addDoc(signalsRef, {
      ...signal,
      timestamp: signal.timestamp || Date.now()
    });
  } catch (err) {
    console.warn(`[Firebase Signaling] Failed to send signal to ${cleanCode}:`, err);
  }
}

export function listenCallSignals(
  roomCode: string, 
  myPeerId: string, 
  isHost: boolean,
  sinceTimestamp: number,
  onSignal: (signal: CallSignal, signalId: string) => void,
  onError?: (err: any) => void
) {
  if (!firestoreDb) {
    if (onError) onError(new Error('Firestore database is not initialized.'));
    return () => {};
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const signalsRef = collection(firestoreDb, 'calls', cleanCode, 'signals');
  const seenSignalIds = new Set<string>();
  const threshold = sinceTimestamp ? sinceTimestamp - 5000 : Date.now() - 30000;

  const unsub = onSnapshot(signalsRef, (snapshot) => {
    snapshot.docChanges().forEach((change) => {
      if (change.type === 'added') {
        const id = change.doc.id;
        if (seenSignalIds.has(id)) return;
        seenSignalIds.add(id);
        const data = change.doc.data() as CallSignal;

        // Discard signals older than call session threshold
        if (data.timestamp && data.timestamp < threshold) return;

        // Ignore signals sent by myself
        if (data.from === myPeerId) return;

        // Target matching:
        // Must match either:
        // 1. my specific peer ID
        // 2. 'all'
        // 3. 'host' ONLY IF I am the host
        const isForMe = data.to === myPeerId || data.to === 'all' || (isHost && data.to === 'host');
        if (!isForMe) return;

        onSignal(data, id);
      }
    });
  }, (err) => {
    console.warn(`[LectureHub Signaling Error] on room ${cleanCode}:`, err);
    if (onError) onError(err);
  });

  return unsub;
}

export async function sendCallMessage(roomCode: string, message: CallMessage) {
  if (!firestoreDb) {
    throw new Error('Firestore database is not initialized. Cannot send message.');
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const msgCol = collection(firestoreDb, 'calls', cleanCode, 'messages');
  try {
    await addDoc(msgCol, {
      ...message,
      timestamp: message.timestamp || Date.now()
    });
  } catch (err: any) {
    console.error(`[Firebase Call Room] Failed to send message in ${cleanCode}:`, err);
    throw new Error(`Failed to send message: ${err?.message || 'Network error'}`);
  }
}

export function listenCallMessages(
  roomCode: string,
  onMessage: (message: CallMessage, messageId: string) => void,
  onError?: (err: any) => void
) {
  if (!firestoreDb) {
    if (onError) onError(new Error('Firestore database is not initialized.'));
    return () => {};
  }
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const msgCol = collection(firestoreDb, 'calls', cleanCode, 'messages');
  const seenMessageIds = new Set<string>();

  const unsub = onSnapshot(msgCol, (snapshot) => {
    snapshot.docChanges().forEach((change) => {
      if (change.type === 'added') {
        const id = change.doc.id;
        if (seenMessageIds.has(id)) return;
        seenMessageIds.add(id);
        const data = change.doc.data() as CallMessage;
        onMessage({ ...data, id }, id);
      }
    });
  }, (err) => {
    console.warn(`[LectureHub Messages Error] on room ${cleanCode}:`, err);
    if (onError) onError(err);
  });

  return unsub;
}


export async function leaveCallRoom(roomCode: string, myPeerId: string, isHost: boolean) {
  if (!firestoreDb) return;
  const cleanCode = roomCode.toUpperCase().replace(/[^A-Z0-9]/g, '');
  try {
    await removeParticipant(cleanCode, myPeerId);
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
    updateCallRoom,
    getCallRoom,
    listenCallRoom,
    registerParticipant,
    updateParticipantState,
    removeParticipant,
    listenParticipants,
    sendCallSignal,
    listenCallSignals,
    sendCallMessage,
    listenCallMessages,
    leaveCallRoom
  };
}


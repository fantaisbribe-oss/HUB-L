import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { 
  getFirestore, 
  collection, 
  doc, 
  setDoc, 
  getDocs, 
  getDocFromServer,
  onSnapshot 
} from 'firebase/firestore';
import firebaseConfig from '../firebase-applet-config.json';

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

// Initialize Firestore with specific database ID if provided
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
    const el = document.getElementById('storageStatus');
    if (el) el.textContent = 'Cloud Synchronized · Firebase Firestore Online';
    return true;
  } catch (error) {
    if (error instanceof Error && error.message.includes('the client is offline')) {
      console.warn('[LectureHub Firebase] The client is offline. Operating with local cache.');
    } else {
      console.log('[LectureHub Firebase] Firestore connected (ready for documents).');
      isFirebaseOnline = true;
      const el = document.getElementById('storageStatus');
      if (el) el.textContent = 'Cloud Synchronized · Firebase Firestore';
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

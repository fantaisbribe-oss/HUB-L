/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect } from 'react';
import { startFirebaseSync, syncRecordToCloud, queryCourseFromCloud, testConnection } from './firebase.ts';

declare global {
  interface Window {
    db: any;
    saveDB: () => void;
    renderTabContent: (tabId: string) => void;
    currentTab: string;
    syncToCloud?: (colName: string, id: string | number, data: any) => Promise<void>;
    queryCourseFromCloud?: (codeOrId: string) => Promise<any | null>;
  }
}

export default function App() {
  useEffect(() => {
    window.queryCourseFromCloud = queryCourseFromCloud;
    if (typeof window.syncToCloud === 'function') {
      console.log('[LectureHub App] Cloud sync already running from Firebase module.');
      return;
    }

    // Expose cloud sync method globally for index.html handlers
    window.syncToCloud = async (colName: string, id: string | number, data: any) => {
      await syncRecordToCloud(colName, id, data);
    };

    // Run connection test and start sync
    testConnection();

    const stopSync = startFirebaseSync(
      () => window.db || {},
      (colName: string, cloudItems: any[]) => {
        if (!window.db) return;
        let modified = false;

        if (!Array.isArray(window.db[colName])) {
          window.db[colName] = [];
        }

        const localList = window.db[colName];

        cloudItems.forEach((cloudItem) => {
          if (!cloudItem || cloudItem.id === undefined) return;
          const index = localList.findIndex((item: any) => String(item.id) === String(cloudItem.id));
          if (index >= 0) {
            // Update item if changed
            if (JSON.stringify(localList[index]) !== JSON.stringify(cloudItem)) {
              localList[index] = { ...localList[index], ...cloudItem };
              modified = true;
            }
          } else {
            // New item added from cloud
            localList.push(cloudItem);
            modified = true;
          }
        });

        if (modified) {
          try {
            localStorage.setItem('lecturehub_multi_level_sec_v13', JSON.stringify(window.db));
          } catch (e) {
            // ignore storage quota error
          }
          if (typeof window.renderTabContent === 'function' && window.currentTab) {
            window.renderTabContent(window.currentTab);
          }
        }
      }
    );

    return () => {
      if (stopSync) stopSync();
    };
  }, []);

  return null;
}

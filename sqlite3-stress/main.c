/* Sqlite3 busy test
 * Multiple threads writing to a database
 *
 * Copyright (c) 2015, Carlos Tangerino
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of SqliteBusyTest nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "sqlite3.h"

/**
    The database definition
 
 CREATE TABLE busy(id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
                   thread integer,
                   Created  datetime DEFAULT CURRENT_TIMESTAMP,
                   sequence integer);
 */

#define MAXTHREAD 10
#define MINBUSYTIME 2000

struct {
    int threadParam;
    pthread_t threadId;
} aux[MAXTHREAD];

static int execSql(sqlite3 *db, const char *sql) {
    int rc = SQLITE_OK;
    if (db == NULL) {
        return 1;
    }
    rc = sqlite3_exec(db, sql, NULL, 0, NULL);
    if (rc && (rc != SQLITE_CONSTRAINT)) {
        printf("Error %d (%s) Query:%s\n", rc, sqlite3_errmsg(db), sql);
    }
    return rc;
}


static int openDatabase(sqlite3 **db, const char *databaseName) {
    int rc = sqlite3_open(databaseName, db);
    if (rc == SQLITE_OK) {
        const char *pragma = "PRAGMA journal_mode = WAL;"
                             "PRAGMA foreign_keys = ON;";
                             // we may add a static parameter here -> "PRAGMA busy_timeout = 30000;";
        rc = sqlite3_exec(*db, pragma, NULL, 0, NULL);
        if (rc != SQLITE_OK) {
            printf("%s (%s)", sqlite3_errmsg(*db), databaseName);
        } else {
            int timeOut = MAXTHREAD * 200;
            if (timeOut < MINBUSYTIME) {
                timeOut = MINBUSYTIME;     // at least one second time out
            }
            printf ("Busy time out set to %d\n", timeOut);
            rc = sqlite3_busy_timeout (*db, timeOut);
        }
    } else {
        printf("CAN'T OPEN DATABASE %s - %s", databaseName, sqlite3_errstr(rc));
    }
    return rc;
}

static int stress (sqlite3 *db, int threadId, int inserts) {
    int rc = 0;
    const char *insert = "insert into busy (thread, sequence) values (%d,%d);";
    char query[1000];
    int count;
    execSql (db, "BEGIN IMMEDIATE");
    for (count = 1; count <= inserts; count++) {
        sprintf (query, insert, threadId, count);
        rc = execSql (db, query);
        if (rc != SQLITE_OK) {
            printf ("Aborting thread %d after %d iterations\n", threadId, count);
            break;
        }
    }
    execSql (db, "COMMIT");
    return rc;
}

static int delete (sqlite3 *db) {
    int rc = 0;
    const char *delete = "delete from busy;";
    int count;
    for (count = 1; count <= 5; count++) {
        sleep (1);
        execSql (db, "BEGIN IMMEDIATE");
        rc = execSql (db, delete);
        execSql (db, "COMMIT");
        if (rc != SQLITE_OK) {
            printf ("Aborting thread DELETE after %d iterations\n", count);
            break;
        }
    }
    return rc;
}

void busyThread(void *arg) {
    int threadId = *(int *)arg;
    sqlite3 *db = NULL;
    int rc = openDatabase(&db, "../busy.db3");
    if (rc == SQLITE_OK) {
        printf ("Thread %d started\n", threadId);
        if (threadId == 1) {
            rc = delete(db);
        } else {
            rc = stress (db, threadId, 1000);
        }
        printf ("Thread %d finished\n", threadId);
    }
}

int main(int argc, char** argv) {
    int threads;
    for (threads = 0; threads < MAXTHREAD; threads++) {
        aux[threads].threadId = (pthread_t) 0;
        aux[threads].threadParam = threads + 1;
        pthread_create(&aux[threads].threadId, NULL, (void *) &busyThread, (void *) &aux[threads].threadParam);
    }
    for (threads = 0; threads < MAXTHREAD; threads++) {
        pthread_join(aux[threads].threadId, NULL);
    }
    return (EXIT_SUCCESS);
}

